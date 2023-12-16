import os
from typing import Any
import httpx
import json
from base64 import b64decode, b64encode
from urllib.parse import unquote, quote
from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd
from pandas import DataFrame
from typing import Optional
from loguru import logger
import time
from b3.utils import save_json, load_json


class Acao(BaseModel):
    sector: str
    segmentTipe: str
    segment: str
    codeCVM: int
    issuingCompany: str
    companyName: str
    tradingName: str
    cnpj: int
    marketIndicator: int
    typeBDR: str
    dateListing: str
    status: str
    segment: str
    segmentEng: str
    type: int
    market: str


class Crawler:
    def __init__(self) -> None:
        self.base_url = (
            "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
        )
        self.search_endpoint = "GetIndustryClassification/"
        self.results_endpoint = "GetInitialCompanies/"

    def decode_params(self, params: str | bytes) -> Any:
        return json.loads(b64decode(params).decode("utf-8"))

    def encode_params(self, params: dict) -> str:
        return b64encode(str(json.dumps(params).replace(" ", "")).encode()).decode(
            "utf-8"
        )

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

        url_params = self.encode_params(params)

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
        resps = {}

        resp = self.load_search()

        for i in tqdm(resp):
            for j in i["subSectors"]:
                for segment in j["segment"]:
                    resps[segment] = self.load_industries(segment)

        logger.success("Done!")
        print(resps)


if __name__ == "__main__":
    c = Crawler()
    c.fetch_all()
