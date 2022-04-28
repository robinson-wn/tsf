import logging

from tsf.spark import get_spark

__name__ = "tsf"

logging.basicConfig(format='%(asctime)s %(levelname)s: %(name)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
