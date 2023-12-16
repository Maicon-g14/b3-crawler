from b3.crawler import Crawler
from loguru import logger

def main():
    logger.info("B3-Crawler")
    
    crawler = Crawler()
    crawler.fetch_all()

if __name__ == "__main__":
    main()