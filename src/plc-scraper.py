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
    table1 = convert_table(table.table)


    table2 = soup.find("div", class_="multiple-version-table")
    table2 = prune_version_table(table2)

    metadata = get_metadata(soup, title)


def convert_table(table: BeautifulSoup):
    """
    Convert a technical support table from the Esri support site into a simplified list of bullet points in the form:
    
    Technical support Version 3.6.2 release date February 19, 2026
    • General Availability, February 19, 2026—May 31, 2027: Create case, online support, software updates, new env certs
    • Mature June 01, 2027—November 30, 2028: Create case, online support
    • Retirement December 01, 2028: online support
    """
    caption = table.caption
    row_headers = [tag.get_text().strip() for tag in table.tbody.find_all("th")]
    row_contents = [[content for content in row.find_all("td")] for row in table.tbody.find_all("tr")]

    result = "<h2>" + caption.get_text(" ").strip() + ". Available actions and resources for each life cycle stage:</h2>\n<ul>\n"

    for col_number, col_header in enumerate(table.thead.find_all("th")):
        supported_actions = []

        column_name = col_header.get_text(" ").strip()

        # for i in len rows, check if row_contents has checkmark, add row header to supported actions
        for row_number, row in enumerate(row_contents):
            if row[col_number].find("calcite-icon"): # aka checkmark
                supported_actions.append(row_headers[row_number])

        result += f"<li>{column_name}: {', '.join(supported_actions)}</li>\n"
    
    result = result + "</ul>\n"
    return result


def prune_version_table(table: BeautifulSoup):
    """
    Prune the html of the version table from the support site to exclude all color and styling info and any retired versions
    """

    # Prune Table Headers
    for header in table.thead.find_all("th"):
        header.attrs = {}
    
    # Prune Table Body
    for row in table.tbody.find_all("tr"):
        # Remove retired rows:
        if "class" in row.attrs and "hide-retired" in row["class"]:
            row.decompose()
            continue

        #Prune 
        for cell in row.descendants:
            if cell.name == "calcite-link":
                cell.unwrap()

            cell.attrs = {}

    # remove "show retired" button
    button = table.find("div", class_="display-retired")
    if button:
        button.decompose()

    return table

def get_metadata(soup, title):
    product = get_product_tag(soup, plc=True)
    last_modified = get_date_tag(soup)

    metadata = make_metadata(product, last_modified, url, title)

    return metadata


if __name__ == "__main__":
    content = asyncio.run(fetch_url_source(url))
    soup = BeautifulSoup(content, 'html.parser')
    get_content(soup)