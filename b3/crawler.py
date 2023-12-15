import httpx
import json
from base64 import b64decode, b64encode
from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd


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
        pass

    def decode_params(self, params: str | bytes) -> dict | list:
        return json.loads(b64decode(params).decode("utf-8"))

    def encode_params(self, params: dict | list | str) -> str:
        return b64encode(str(json.dumps(params).replace(" ", "")).encode()).decode(
            "utf-8"
        )

    def save_json(self, file, name):
        with open(f"{name}.json", "w") as f:
            f.write(json.dumps(file))

    def req(self, url, params=None):
        res = httpx.get(url, params=params, timeout=120)

        res.raise_for_status()

        return json.loads(res.text)

    def fetch(self):
        resps = {}

        for i in tqdm(resp):
            for j in i["subSectors"]:
                for segment in j["segment"]:
                    resps[segment] = get_segment(segment)

        save_json(resps, "out")

        print("done!")


def main():
    base_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetIndustryClassification/"
    endpoint = "eyJsYW5ndWFnZSI6InB0LWJyIn0="

    res = httpx.get(base_url + endpoint)
    res.raise_for_status()

    resp = json.loads(res.text)


if __name__ == "__main__":
    main()
