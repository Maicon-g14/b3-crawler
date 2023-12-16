import time
import json
from urllib.parse import quote
from typing import Any, Optional

from pandas import DataFrame
import httpx
from tqdm import tqdm
from pydantic import BaseModel
from loguru import logger

from b3.utils import save_json, load_json, encode_params


class Stock(BaseModel):
    codeCVM: int
    sector: str
    segmentTipe: str
    segment: str
    segmentEng: str
    issuingCompany: str
    companyName: str
    tradingName: str
    cnpj: int
    marketIndicator: int
    typeBDR: str
    dateListing: str
    status: str
    type: int
    market: str


class Crawler:
    def __init__(self, final_filepath: str = "data/stocks.csv") -> None:
        self.base_url = (
            "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
        )
        self.search_endpoint = "GetIndustryClassification/"
        self.results_endpoint = "GetInitialCompanies/"
        self.final_filepath = final_filepath

    def req_page(
        self,
        url: str,
        params: Optional[str] = None,
        proxy: Optional[str] = None,
        retries: int = 5,
    ) -> Any:
        try:
            res = httpx.get(url, params=params, timeout=120)

            res.raise_for_status()

            return json.loads(res.text)
        except Exception as e:
            if retries > 0:
                logger.warning(f"Couldn't fetch {url=} - {e} - {retries=}")
                time.sleep(5)
                return self.req_page(url, params, proxy, retries - 1)

            logger.error(f"Couldn't fetch {url=} - {e}")

    def fetch_industries(self, segment: str) -> Any:
        params = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 100,
            "segment": quote(segment),
        }

        url_params = encode_params(params)

        res = self.req_page(self.base_url + self.results_endpoint + url_params)

        save_json(res, segment)

        return res

    def fetch_search(self) -> Any:
        res = self.req_page(
            f"{self.base_url}{self.search_endpoint}eyJsYW5ndWFnZSI6InB0LWJyIn0="
        )

        save_json(res, "search")
        return res

    def load_search(self) -> Any:
        try:
            return load_json("search")
        except Exception:
            return self.fetch_search()

    def load_industries(self, segment: str) -> Any:
        try:
            return load_json(segment)
        except Exception:
            logger.info("Local data not found! Downloading...")
            return self.fetch_industries(segment)

    def fetch_all(self):
        stocks = []

        logger.info("Starting crawler...")

        resp = self.load_search()

        for i in tqdm(resp):
            for j in i["subSectors"]:
                for segment in j["segment"]:
                    industries = self.load_industries(segment)

                    for stock in industries["results"]:
                        stocks.append(
                            Stock(
                                codeCVM=int(stock["codeCVM"]),
                                sector=i["sector"],
                                segmentTipe=j["describle"],
                                segment=segment,
                                segmentEng=stock["segmentEng"],
                                issuingCompany=stock["issuingCompany"],
                                companyName=stock["companyName"],
                                tradingName=stock["tradingName"],
                                cnpj=int(stock["cnpj"]),
                                marketIndicator=int(stock["marketIndicator"]),
                                typeBDR=stock["typeBDR"],
                                dateListing=stock["dateListing"],
                                status=stock["status"],
                                type=int(stock["type"]),
                                market=stock["market"],
                            ).model_dump()
                        )

        res = DataFrame(stocks).set_index("codeCVM")
        res.to_csv(self.final_filepath)

        logger.success("Done!")
        logger.info(f"Final data saved in \"{self.final_filepath}\"")


if __name__ == "__main__":
    c = Crawler()
    c.fetch_all()
