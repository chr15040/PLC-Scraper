from datetime import datetime
from dateutil import parser
import json
import re


MAX_LENGTH = 30000


def chunk_text_by_sentence(text):
    sentences = re.split(r'(?<=[.!?]) +', text.strip())
    current_chunk = []
    curr_length = 0

    for sentence in sentences:
        sentence = sentence.strip()
        sentence_length = len(sentence)

        if sentence_length > MAX_LENGTH:
            if current_chunk:
                yield " ".join(current_chunk)
                current_chunk = []
                curr_length = 0

            for i in range(0, sentence_length, MAX_LENGTH):
                yield sentence[i:i+MAX_LENGTH]
            continue

        if curr_length + sentence_length + (1 if current_chunk else 0) <= MAX_LENGTH:
            current_chunk.append(sentence)
            curr_length += sentence_length + (1 if current_chunk else 0)
        else:
            yield " ".join(current_chunk)
            current_chunk = [sentence]
            curr_length = sentence_length

    if current_chunk:
        yield " ".join(current_chunk)


def get_date_tag(soup):
    date_tag = soup.find('meta', attrs={'name': 'last-modified'})
    if date_tag and 'content' in date_tag.attrs:
        try:
            parsed = parser.parse(date_tag['content'])
            return parsed.isoformat()
        except (ValueError, TypeError):
            pass
    return None


def get_product_tag(soup, plc=False):
    product = None
    product_tag = soup.find('meta', attrs={'name': 'product'})
    if product_tag and 'content' in product_tag.attrs:
        product = product_tag['content']
    if plc and product:
        product = product + ", life cycle"
    return product


def make_metadata(product, last_modified, url, title, prod_version=None, region='general'):
    metadata = {}

    if product:
        tags = product.lower()
        tags = re.sub(r'\bbug-\d+\b', lambda m: m.group(0).replace('-', '_'), tags)
        tags= tags.replace('-', ' ')
        tags = re.sub(r'\s*,\s*', ',', tags)
        tags = re.sub(r'\b\d+(\.\d+)*', '', tags)
        tags = re.sub(r'(?<!\w)\s*x\s*(?!\w)', '', tags)
        tags = re.sub(r'\b\d+\s*x\b', '', tags)
        tags = re.sub(r'\bx\s*\d+(\.\d+)*\b', '', tags).strip()
        tags = re.sub(r'\s*$', '', tags)
        tags = re.sub(r'\s*,', ',', tags)
        tags = tags.replace('_', ' ')
        tags = re.sub(r'\s+', ' ', tags).strip()
    else:
        tags = None

    if tags and ',' in tags:
        tag_list = tags.split(',')
        clean_tags = list(set(tag_list))
    elif tags:
        clean_tags = []
        clean_tags.append(tags)
    else:
        clean_tags = None


    metadata['tags'] = clean_tags if clean_tags else None
    metadata['prod_version'] = prod_version if prod_version else None
    metadata['published_date'] = last_modified if last_modified else datetime.now().replace(microsecond=0).isoformat()
    metadata['indexed_date'] = datetime.now().replace(microsecond=0).isoformat()
    metadata['source'] = url
    metadata['region'] = region

    if isinstance(title, str):
        metadata['title'] = title
    else:
        metadata['title'] = title.get_text().strip() if title else None
    metadata_str = json.dumps(metadata)
    return metadata_str


def remove_duplicate_newlines(s):
    return re.sub(r'(\n)+', r'\n', s)


def remove_newlines(series):
    series = series.astype(str)
    series = series.str.replace('\n', ' ')
    series = series.str.replace('\\n', ' ')
    series = series.str.replace('  ', ' ')
    series = series.str.replace('  ', ' ')
    return series
