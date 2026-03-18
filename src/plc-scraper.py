import asyncio

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import csv
import re

from utils.scraper_utils import *

INPUT_CSV = "plcs_to_scrape.csv"
OUTPUT_CSV = "src/newprocessed/scraped_plc.csv"


def scrape():
    with open(INPUT_CSV, newline='', encoding='utf-8-sig') as plcs_to_scrape, \
        open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as scraped_plcs:

        reader = csv.DictReader(plcs_to_scrape)
        writer = csv.writer(scraped_plcs)
        writer.writerow(["metadata", "content"])

        for row in reader:
            url = row['url'].strip()

            raw_content = asyncio.run(fetch_url_source(url))

            soup = BeautifulSoup(raw_content, 'html.parser')
            content = get_content(soup)
            metadata = get_metadata(soup, url)

            clean_content = clean_text(content)
            writer.writerow([metadata, clean_content])


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
    """
    Get the scraped content from the page
    """
    title = soup.find("h1", class_="esri-text__title")
    subtitle = soup.find("h2", class_="esri-text__title")
    
    table1 = convert_tech_supt_table(soup)

    table2 = prune_version_table(soup)

    add_prod_info = get_additional_prod_info(soup)
    
    content = f"{title} {subtitle} {table1} {table2} {add_prod_info}"

    return content


def convert_tech_supt_table(soup: BeautifulSoup):
    """
    Convert a technical support table from the Esri support site into a simplified list of bullet points in the form:
    
    Technical support Version 3.6.2 release date February 19, 2026
    • General Availability, February 19, 2026—May 31, 2027: Create case, online support, software updates, new env certs
    • Mature June 01, 2027—November 30, 2028: Create case, online support
    • Retirement December 01, 2028: online support
    """
    table_div = soup.find("div", class_="technical-support-table")
    table = table_div.find("div", class_="table-wrapper")

    caption = table.caption
    row_headers = [tag.get_text().strip() for tag in table.tbody.find_all("th")]
    row_contents = [[content for content in row.find_all("td")] for row in table.tbody.find_all("tr")]

    result = "<h2>" + caption.get_text(" ").strip() + ". Available actions and resources for each life cycle stage:</h2><ul>"

    for col_number, col_header in enumerate(table.thead.find_all("th")):
        supported_actions = []

        column_name = col_header.get_text(" ").strip()

        # for i in len rows, check if row_contents has checkmark, add row header to supported actions
        for row_number, row in enumerate(row_contents):
            if row[col_number].find("calcite-icon"): # aka checkmark
                supported_actions.append(row_headers[row_number])

        result += f"<li>{column_name}: {', '.join(supported_actions)}</li>"
    
    result = result + "</ul>"
    return result


def prune_version_table(soup: BeautifulSoup):
    """
    Prune the html of the version table from the support site to exclude all color and styling info and any retired versions
    """
    table = soup.find("div", class_="multiple-version-table")

    table.attrs = {}
    table.h2.attrs = {}
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


def get_additional_prod_info(soup: BeautifulSoup):
    """
    Extract and prune the additional product information section from the page if there is one
    """
    section_header = soup.find("div", class_="columnsystem").get_text().strip()
    if not section_header == "Additional product information":
        return ""

    result = ""
    
    for tag in soup.find_all("div", class_="columnsystem", limit=2):
        prod_info_text = tag.get_text("").strip()
        prod_info_text = remove_duplicate_newlines(prod_info_text)
        result += prod_info_text + ""
    
    return "<p>" + result + "</p>"


def get_metadata(soup, url):
    title = soup.find("h1", class_="esri-text__title")
    product = get_product_tag(soup, plc=True)
    last_modified = get_date_tag(soup)

    metadata = make_metadata(product, last_modified, url, title)

    return metadata


def clean_text(text):
    clean_text = re.sub(r'(\n)+', ' ', text)
    clean_text = clean_text.replace('\u00a0', ' ')
    return clean_text

if __name__ == "__main__":
    scrape()
