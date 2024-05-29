import logging


error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler('error.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)


links_logger = logging.getLogger('links_logger')
links_logger.setLevel(logging.INFO)
links_handler = logging.FileHandler('links.log')
links_handler.setLevel(logging.INFO)
links_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
links_handler.setFormatter(links_formatter)
links_logger.addHandler(links_handler)


detail_links_logger = logging.getLogger('detail_links_logger')
detail_links_logger.setLevel(logging.DEBUG)
detail_links_handler = logging.FileHandler('detail_links.log')
detail_links_handler.setLevel(logging.DEBUG)
detail_links_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
detail_links_handler.setFormatter(detail_links_formatter)
detail_links_logger.addHandler(detail_links_handler)
