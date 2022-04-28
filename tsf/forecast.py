import base64
from io import BytesIO
from typing import Tuple

import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.plot import add_changepoints_to_plot, plot_cross_validation_metric

from tsf import logger

# Consider replacing with newer version of Prophet: https://towardsdatascience.com/how-to-use-facebooks-neuralprophet-and-why-it-s-so-powerful-136652d2da8b



def state_forecast_udf(history: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function for running state_forecast as UDF and passing arguments.
    Note that column order is important: State, Date, DV, additional regressors...
    :param history: data history DataFrame. Ordered columns are: state, date, deaths, additional regressors
    :return: history DataFrame unchanged.
    """
    return state_forecast(history, periods=90)


def state_forecast(history: pd.DataFrame, periods=90) -> pd.DataFrame:
    """
    Computes a forecast from a DataFrame having columns State, Date, DV, additional regressors...
    The DV (dependent variable) is being predicted. For the COVID data, DV is deaths and additional
    regressors are cases.
    As output, it writes out an forecast image for each state.
    :param history: data history DataFrame. Ordered columns are: state, date, deaths, additional regressors
    :return: deaths_forecast DataFrame.
    """
    state = history['state'].iloc[0]
    date_column = 'date'
    last_date = history[date_column].iat[-1]

    # For each extra regressor column, create forecasts based on current data.
    extra_regressors = []
    for column in history.columns[3:]:
        logger.debug(f"Forecasting extra regressor {column} for {state}")
        forecast, _ = generate_forecast(history[[date_column, column]], periods=periods)
        forecast.rename(columns={'yhat': column}, inplace=True)
        extra_regressors += [forecast[[column]]]
    forecast_regressors = pd.concat(extra_regressors, axis=1)

    # without the state column, use forecasted data as input to next forecast.
    logger.debug(f"Forecasting deaths for {state}")
    deaths_forecast, deaths_model = generate_forecast(history.iloc[:, 1:], future_regressors=forecast_regressors,
                                                      periods=periods)

    # Summarize average change over period
    past_mean = float(np.mean(deaths_forecast.tail(2 * periods).head(periods)[['yhat']]))
    forecast_mean = float(np.mean(deaths_forecast.tail(periods)[['yhat']]))
    mean_change = (forecast_mean - past_mean) / past_mean
    summary = f"Forecast {mean_change * 100:,.1f}% change over next {periods} days, from {past_mean:,.1f} to {forecast_mean:,.1f}"
    logger.info(f"State: {state} {summary}")

    # Create figures
    figure_forecast = prediction_fig(deaths_model, deaths_forecast, title=f"{state}\n{summary}", y_label='deaths')
    logger.debug(f"Computing forecast metrics for {state}")
    df_cv = cross_validation(deaths_model, period='90 days', horizon='180 days', parallel='threads')
    figure_metrics = plot_cross_validation_metric(df_cv, metric='rmse')
    figure_metrics.axes[0].set_title(f"{state} RMSE", {'fontsize': 16}, y=1, pad=-40)

    state_summary = {'state': state, 'date': last_date, 'past_mean': past_mean, 'forecast_mean': forecast_mean,
                     'mean_change': mean_change, 'forecast_fig': figure_to_base64(figure_forecast),
                     'metrics_fig': figure_to_base64(figure_metrics)}
    return pd.DataFrame(state_summary, index=[0])


# https://stackoverflow.com/questions/63580039/how-to-convert-pyplot-image-in-bytes
def figure_to_base64(figure: Figure) -> bytes:
    """
    Convert matplotlib.figure to a string, which can be placed in a DataFrame
    :param figure: matplotlib.figure
    :return: base64 string representing figure as png
    """
    byte_io = BytesIO()
    figure.savefig(byte_io, format='png')
    byte_io.seek(0)
    return base64.b64encode(byte_io.read())


def write_base64_image(base64_str: str, path: str):
    """
    Writes figure to file.
    :param base64_str: matplotlib.figure png as base64 string
    :param path: Write file path.
    :return:
    """
    file = open(path, "wb")
    file.write(base64.b64decode(base64_str))
    file.close()


# https://facebook.github.io/prophet/docs/seasonality,_holiday_effects,_and_regressors.html#additional-regressors
def generate_forecast(history: pd.DataFrame,
                      future_regressors: pd.DataFrame = None,
                      periods=90) -> Tuple[pd.DataFrame, Prophet]:
    """
    Generates a Prophet forecast from the `history` and any additional regressors.
    If there are future_regressors, then those values will be used as the forecasted regressors values.
    :param history: Prophet history DataFrame. Columns are: dates, y (DV), additional regressors
    :param future_regressors: If provided, future_regressors values will be used as the forecasted regressors values.
    :param periods: Prophet forecast period.
    :return: Prophet forecast Dataframe, Prophet model
    """
    # Required column names for Prophet (ds, y)
    history = history.rename(columns={history.columns[0]: 'ds', history.columns[1]: 'y'})
    model = Prophet(
        changepoint_prior_scale=0.5,
        # interval_width=0.95,
        growth='linear',
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=True,
        seasonality_mode='multiplicative'
    )

    assert len(history.columns) == 2 or (len(history.columns) > 2 and not future_regressors.empty), \
        "If >2 columns, then must provide future_regressors in DataFrame"

    # Has additional regressor columns
    if len(history.columns) > 2 and not future_regressors.empty:
        for col in history.columns[2:]:
            model.add_regressor(col)
    elif len(history.columns) > 2:
        # Only use dates and y column (without future_regressors)
        history = history.iloc[:, :2]
    model.fit(history)

    forecast = model.make_future_dataframe(
        periods=periods,
        freq='d',
        include_history=True
    )

    # Use historical data, where available. Then, add forecast rows from future_regressors.
    if len(history.columns) > 2 and not future_regressors.empty:
        # historical data without the date and y columns
        historical = history.iloc[:, 2:]
        added_rows = forecast.shape[0] - history.shape[0]
        # Add forecast rows to historical data to form forecast data with the added regressors
        historical_and_regressors = pd.concat([historical, future_regressors.tail(added_rows)], axis=0)
        forecast = pd.concat([forecast, historical_and_regressors], axis=1)

    prediction = model.predict(forecast)
    return prediction, model


def prediction_fig(model: Prophet, forecast: pd.DataFrame, title=None, y_label='deaths', change_points=False) -> Figure:
    """
    Return a Figure showing the historical data and the forecast.
    :param model: Prophet forecast model.
    :param forecast: DataFrame of Prophet forecast
    :param title: Figure title
    :param y_label: Figure y axis label
    :return: Figure
    """
    figure = model.plot(forecast, xlabel='date', ylabel=y_label)
    # https://facebook.github.io/prophet/docs/trend_changepoints.html#automatic-changepoint-detection-in-prophet
    if change_points:
        add_changepoints_to_plot(figure.gca(), model, forecast)
    figure.axes[0].set_title(title, {'fontsize': 16}, y=1, pad=-40)
    return figure
