import asyncio

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from utils.scraper_utils import get_date_tag, get_product_tag, make_metadata

url = "https://support.esri.com/en-us/products/arcgis-pro/life-cycle"


async def fetch_url_source(url):
    for _ in range(3):
        try:
            async with Stealth().use_async(async_playwright()) as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                await page.goto(url, wait_until="load")

                content = await page.content()
                await browser.close()
                return content
        except Exception as e:
            print(f"Error fetching {url}: {str(e)}")


def get_content(soup):
    title = soup.find("h1", class_="esri-text__title")
    subtitle = soup.find("h2", class_="esri-text__title")
    # print(title)
    # print(subtitle)

    table1 = soup.find("div", class_="technical-support-table")
    table = table1.find("div", class_="table-wrapper")
    print(table.table)


    table2 = soup.find("div", class_="multiple-version-table")

    metadata = get_metadata(soup, title)


def get_metadata(soup, title):
    product = get_product_tag(soup, plc=True)
    last_modified = get_date_tag(soup)

    metadata = make_metadata(product, last_modified, url, title)

    return metadata


if __name__ == "__main__":
    content = asyncio.run(fetch_url_source(url))
    soup = BeautifulSoup(content, 'html.parser')
    get_content(soup)