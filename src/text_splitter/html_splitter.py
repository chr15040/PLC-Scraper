from __future__ import annotations
import re
import types
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from bs4 import Tag, NavigableString
from langchain_core.documents import BaseDocumentTransformer, Document
from markdownify import MarkdownConverter

from .recursive_splitter import RecursiveCharacterTextSplitter
from .translator import translate_text


class HTMLSemanticPreservingSplitter(BaseDocumentTransformer):
    """
    Args:
        headers_to_split_on (List[Tuple[str, str]]): HTML headers (e.g., "h1", "h2")
            that define content sections.
        max_chunk_size (int): Maximum size for each chunk, with allowance for
            exceeding this limit to preserve semantics.
        chunk_overlap (int): Number of characters to overlap between chunks to ensure
            contextual continuity.
        separators (List[str]): Delimiters used by RecursiveCharacterTextSplitter for
            further splitting.
        elements_to_preserve (List[str]): HTML tags (e.g., <table>, <ul>) to remain
            intact during splitting.
        preserve_links (bool): Converts <a> tags to Markdown links ([text](url)).
        preserve_images (bool): Converts <img> tags to Markdown images (![alt](src)).
        preserve_videos (bool): Converts <video> tags to Markdown
        video links (![video](src)).
        preserve_audio (bool): Converts <audio> tags to Markdown
        audio links (![audio](src)).
        custom_handlers (Dict[str, Callable[[Any], str]]): Optional custom handlers for
            specific HTML tags, allowing tailored extraction or processing.
        stopword_removal (bool): Optionally remove stopwords from the text.
        stopword_lang (str): The language of stopwords to remove.
        normalize_text (bool): Optionally normalize text
            (e.g., lowercasing, removing punctuation).
        external_metadata (Optional[Dict[str, str]]): Additional metadata to attach to
            the Document objects.
        allowlist_tags (Optional[List[str]]): Only these tags will be retained in
            the HTML.
        denylist_tags (Optional[List[str]]): These tags will be removed from the HTML.
        preserve_parent_metadata (bool): Whether to pass through parent document
            metadata to split documents when calling
            ``transform_documents/atransform_documents()``.
        keep_separator (Union[bool, Literal["start", "end"]]): Whether separators
            should be at the beginning of a chunk, at the end, or not at all.
    """

    def __init__(
        self,
        headers_to_split_on: List[Tuple[str, str]],
        *,
        max_chunk_size: int = 1000,
        chunk_overlap: int = 0,
        separators: Optional[List[str]] = None,
        elements_to_preserve: Optional[List[str]] = None,
        preserve_links: bool = False,
        preserve_images: bool = False,
        preserve_videos: bool = False,
        preserve_audio: bool = False,
        custom_handlers: Optional[Dict[str, Callable[[Any], str]]] = None,
        stopword_removal: bool = False,
        stopword_lang: str = "english",
        normalize_text: bool = False,
        external_metadata: Optional[Dict[str, str]] = None,
        allowlist_tags: Optional[List[str]] = None,
        denylist_tags: Optional[List[str]] = None,
        preserve_parent_metadata: bool = False,
        keep_separator: Union[bool, Literal["start", "end"]] = True,
        translate: Optional[bool] = False,
    ):
        """Initialize splitter."""
        try:
            from bs4 import BeautifulSoup, Tag

            self._BeautifulSoup = BeautifulSoup
            self._Tag = Tag
        except ImportError:
            raise ImportError(
                "Could not import BeautifulSoup. "
                "Please install it with 'pip install bs4'."
            )

        self.chunk_sequence = 0
        self._headers_to_split_on = sorted(headers_to_split_on)
        self._max_chunk_size = max_chunk_size
        self._elements_to_preserve = elements_to_preserve or []
        self._preserve_links = preserve_links
        self._preserve_images = preserve_images
        self._preserve_videos = preserve_videos
        self._preserve_audio = preserve_audio
        self._custom_handlers = custom_handlers or {}
        self._stopword_removal = stopword_removal
        self._stopword_lang = stopword_lang
        self._normalize_text = normalize_text
        self._external_metadata = external_metadata or {}
        self._allowlist_tags = allowlist_tags
        self._preserve_parent_metadata = preserve_parent_metadata
        self._keep_separator = keep_separator
        self._table_placeholders = []
        self._tables = {}
        self._translate = translate

        if allowlist_tags:
            self._allowlist_tags = list(
                set(allowlist_tags + [header[0] for header in headers_to_split_on])
            )
        self._denylist_tags = denylist_tags
        if denylist_tags:
            self._denylist_tags = [
                tag
                for tag in denylist_tags
                if tag not in [header[0] for header in headers_to_split_on]
            ]
        if separators:
            self._recursive_splitter = RecursiveCharacterTextSplitter(
                separators=separators,
                keep_separator=keep_separator,
                chunk_size=max_chunk_size,
                chunk_overlap=chunk_overlap,
                table_callback=lambda: self._tables,
            )
        else:
            self._recursive_splitter = RecursiveCharacterTextSplitter(
                keep_separator=keep_separator,
                chunk_size=max_chunk_size,
                chunk_overlap=chunk_overlap,
                table_callback=lambda: self._tables,
            )

        if self._stopword_removal:
            try:
                import nltk
                from nltk.corpus import stopwords  # type: ignore[import-untyped]

                nltk.download("stopwords")
                self._stopwords = set(stopwords.words(self._stopword_lang))
            except ImportError:
                raise ImportError(
                    "Could not import nltk. Please install it with 'pip install nltk'."
                )

    def split_text(self, text: str) -> List[Document]:
        """Splits the provided HTML text into smaller chunks based on the configuration.

        Args:
            text (str): The HTML content to be split.

        Returns:
            List[Document]: A list of Document objects containing the split content.
        """
        soup = self._BeautifulSoup(text, "html.parser")

        self._extract_tables(soup)

        self._process_media(soup)

        if self._preserve_links:
            self._process_links(soup)

        if self._allowlist_tags or self._denylist_tags:
            self._filter_tags(soup)

        return self._process_html(soup)

    def handle_headers(self, content):
        empty_header_pattern = re.compile(
            r"<h[1-6][^>]*>\s*</h[1-6]>", flags=re.IGNORECASE
        )
        content = empty_header_pattern.sub("", content)

        def replace_heading(match):
            tag = match.group(1)
            header = match.group(2).strip()
            if self._translate:
                header = translate_text(text=header)
            level = int(tag[1])
            return f"\n{'#' * level} {header}\n"

        pattern = re.compile(
            r"<(h[1-6])[^>]*>(.*?)</\1>", flags=re.IGNORECASE | re.DOTALL
        )

        return pattern.sub(replace_heading, content)

    def transform_documents(
        self, documents: Sequence[Document], **kwargs: Any
    ) -> List[Document]:
        """Transform sequence of documents by splitting them."""
        transformed = []
        for doc in documents:
            splits = self.split_text(doc.page_content)
            if self._preserve_parent_metadata:
                splits = [
                    Document(
                        page_content=self.handle_headers(split_doc.page_content),
                        metadata={**doc.metadata, **split_doc.metadata},
                    )
                    for split_doc in splits
                ]
            transformed.extend(splits)
        return transformed

    def _strip_attributes(self, tag: Tag):
        tag.attrs = {}
        for child in tag.find_all(True):
            child.attrs = {}

    def _process_media(self, soup: Any) -> None:
        """Processes the media elements.

        Process elements in the HTML content by wrapping them in a <media-wrapper> tag
        and converting them to Markdown format.

        Args:
            soup (Any): Parsed HTML content using BeautifulSoup.
        """
        if self._preserve_images:
            for img_tag in soup.find_all("img"):
                img_src = img_tag.get("src", "")
                markdown_img = f"![image:{img_src}]({img_src})"
                wrapper = soup.new_tag("media-wrapper")
                wrapper.string = markdown_img
                img_tag.replace_with(wrapper)

        if self._preserve_videos:
            for video_tag in soup.find_all("video"):
                video_src = video_tag.get("src", "")
                markdown_video = f"![video:{video_src}]({video_src})"
                wrapper = soup.new_tag("media-wrapper")
                wrapper.string = markdown_video
                video_tag.replace_with(wrapper)

        if self._preserve_audio:
            for audio_tag in soup.find_all("audio"):
                audio_src = audio_tag.get("src", "")
                markdown_audio = f"![audio:{audio_src}]({audio_src})"
                wrapper = soup.new_tag("media-wrapper")
                wrapper.string = markdown_audio
                audio_tag.replace_with(wrapper)

    def _process_links(self, soup: Any) -> None:
        """Processes the links in the HTML content.

        Args:
            soup (Any): Parsed HTML content using BeautifulSoup.
        """
        for a_tag in soup.find_all("a"):
            a_href = a_tag.get("href", "")
            a_href_lower = a_href.lower()
            a_text = a_tag.get_text(strip=True)
            if "linkedin" in a_href_lower or "viewprofilepage" in a_href_lower:
                a_tag.replace_with("")
                continue
            elif not a_text:
                a_tag.replace_with("")
            else:
                if self._translate:
                    a_text = translate_text(text=a_text)
                markdown_link = f"[{a_text}]({a_href})"
                wrapper = soup.new_tag("link-wrapper")
                wrapper.string = markdown_link
                a_tag.replace_with(markdown_link)

    def _filter_tags(self, soup: Any) -> None:
        """Filters the HTML content based on the allowlist and denylist tags.

        Args:
            soup (Any): Parsed HTML content using BeautifulSoup.
        """
        for tag_name in ["h1", "h2", "h3", "h4"]:
            for tag in soup.find_all(tag_name):
                tag.attrs = {}

        if self._allowlist_tags:
            for tag in soup.find_all(True):
                if tag.name not in self._allowlist_tags:
                    tag.decompose()

        if self._denylist_tags:
            for tag in soup.find_all(self._denylist_tags):
                tag.decompose()

    def _extract_tables(self, soup: Any):
        """Extracts tables from text and replaces them with a placeholder.

        Args:
            soup (Any): Parsed HTML content using BeautifulSoup.
        """

        def _flatten_nested_tables():
            """Converts a nested table to a list joined by <br> elements."""
            for nested in table_tag.find_all("table"):
                flattened_rows = []
                for row in nested.find_all("tr"):
                    cols = [
                        col.get_text(strip=True) for col in row.find_all(["td", "th"])
                    ]
                    if cols:
                        flattened_rows.append(" → ".join(cols))
                replacement = "<br>".join(flattened_rows)
                if replacement:
                    nested.replace_with("<br>" + replacement)
                nested.decompose()
            return

        def _handle_htags():
            """Extracts and preserves table section header as <table-header-insert> element."""
            h_tag = soup.find("h2", attrs={"role": "tablist"})
            if not h_tag:
                for el in table_tag.find_all_previous():
                    if el.name == "section":
                        break
                    if isinstance(el, Tag) and el.name and el.name.startswith("h"):
                        h_tag_text = el.get_text()
                        if h_tag_text:
                            h_tag = el
                            break
            if h_tag:
                heading_tag_name = h_tag.name
                heading_text = h_tag.get_text(strip=True)
                preserved_heading = self._BeautifulSoup(
                    f"<table-header-insert><{heading_tag_name}>{heading_text}</{heading_tag_name}></table-header-insert>",
                    "html.parser",
                )
                if table_tag.thead:
                    table_tag.thead.insert_before(preserved_heading)
                else:
                    table_tag.insert(0, preserved_heading)

        def _handle_captions():
            """Extracts and preserves table section caption as <caption-insert> element."""
            parent_section = table_tag.find_parent("section")
            caption = parent_section.find("caption")
            if caption:
                caption_text = caption.get_text(strip=True)
                preserved_caption = self._BeautifulSoup(
                    f"<caption-insert>{caption_text}</caption-insert>", "html.parser"
                )
                if table_tag.thead:
                    table_tag.thead.insert_before(preserved_caption)
                else:
                    table_tag.insert(0, preserved_caption)
                caption.decompose()

        def _handle_tab_title(tab_title):
            """Extracts and preserves tab title as <tab-title-insert> element."""
            h4 = self._BeautifulSoup(
                f"<tab-title-insert><h4>{tab_title}</h4></tab-title-insert>",
                "html.parser",
            )
            if table_tag.thead:
                table_tag.thead.insert_before(h4)
            else:
                table_tag.insert(0, h4)

        def _insert_placeholder():
            PLACEHOLDER_PREFIX = "TABLE_PLACEHOLDER_"
            placeholder = f"[[[{PLACEHOLDER_PREFIX}{i}]]]"
            self._table_placeholders.append(f"[[[{PLACEHOLDER_PREFIX}{i}]]]")
            self._tables[placeholder] = str(table_tag)
            table_tag.insert_before(
                self._BeautifulSoup(f"<p>{placeholder}</p>", "html.parser")
            )
            table_tag.decompose()

        tab_titles = []
        tab_titles = [a.get_text(strip=True) for a in soup.select("nav.tab-nav a")]

        tab_sections = soup.select(".tab-contents .tab-section")

        for tag in soup.select(".tab-nav"):
            tag.decompose()

        for i, table_tag in enumerate(soup.find_all(True)):
            if table_tag.name == "table":
                if not table_tag.get_text(strip=True):
                    table_tag.decompose()
                    continue

                _flatten_nested_tables()

                tab_title = None
                parent_article = table_tag.find_parent("article", class_="tab-section")
                if parent_article:
                    tab_index = tab_sections.index(parent_article)
                    if 0 <= tab_index < len(tab_titles):
                        tab_title = tab_titles[tab_index]
                    else:
                        tab_title = None

                parent_section = table_tag.find_parent("section")
                if parent_section:
                    _handle_htags()
                    _handle_captions()

                if tab_title:
                    _handle_tab_title(tab_title)

                _insert_placeholder()
        self._preserve_tables()

    def _translate_table_text(self, bsoup_table):
        for cell in bsoup_table.find_all(["th", "td"]):
            for node in list(cell.children):
                if not isinstance(node, NavigableString):
                    continue

                text = node.strip()
                if not text:
                    continue

                if text.isnumeric():
                    continue
                if text == "#":
                    continue

                translated = translate_text(text=text)
                node.replace_with(translated)

    def _preserve_tables(self):
        """Saves table element to _tables."""

        def _custom_convert_tr(self, el, text, parent_tags):
            """Inserts a single newline trailing <tr> elements."""
            row = text.strip()
            return f"|{row}\n" if row else ""

        def _md(html, make_custom_convert_tr=False, **options):
            """Converts html to markdown."""
            converter = MarkdownConverter(
                table_infer_header=True, escape_underscores=False, **options
            )
            if make_custom_convert_tr:
                converter.convert_tr = types.MethodType(_custom_convert_tr, converter)
            return converter.convert(html)

        def _make_thead():
            """Combines table header, tab title, and caption into single table header."""
            bs_header = bsoup_table.find("table-header-insert")
            bs_title = bsoup_table.find("tab-title-insert")
            caption = bsoup_table.find("caption-insert")

            thead = ""
            if self._translate:
                thead += _md(translate_text(str(bs_header))) + "\n" if bs_header else ""
                thead += _md(translate_text(str(bs_title))) + "\n" if bs_title else ""
                thead += _md(translate_text(str(caption))) + "\n" if caption else ""
            else:
                thead += _md(str(bs_header)) + "\n" if bs_header else ""
                thead += _md(str(bs_title)) + "\n" if bs_title else ""
                thead += _md(str(caption)) + "\n" if caption else ""

            if bs_title:
                bs_title.decompose()
            return thead

        def _handle_header_row(thead):
            """Inserts <th> element for row index into header row."""
            header_row = None
            thead_tag = bsoup_table.find("thead")
            if thead_tag:
                bs_table_head = thead_tag.find("tr")
                header_row = bs_table_head
            else:
                header_row = all_rows[0]

            if header_row:
                header_row.insert(0, self._BeautifulSoup("<th>#</th>", "html.parser"))
                if self._translate:
                    thead += _md(translate_text(str(header_row)))
                else:
                    thead += _md(str(header_row))

            has_thead = False
            if bsoup_table.find("thead"):
                has_thead = True
                bsoup_table.find("thead").decompose()
            return thead, has_thead

        def _handle_long_row():
            """Splits long rows according to max chunk size and adds empty <td> elements to preserve table structure."""
            curr_index = 1
            visited = 0
            saved_td = []
            curr_td = ""
            cells = row.find_all("td")
            total_cells = len(cells)

            for cell in cells:
                curr_text = cell.get_text()
                if len(thead) + len(row) + len(curr_text) <= self._max_chunk_size:
                    curr_td += f"<td>{curr_text}</td>"
                    curr_index += 1
                    visited += 1
                else:
                    items = self._recursive_splitter.split_text(curr_text)
                    items = [f"<td>{item}</td>" for item in items]
                    if curr_td:
                        prepend = "<td></td>" * (curr_index - 1 - visited)
                        items = [prepend + curr_td + items[0]] + [
                            f"<td>{row_index}</td>"
                            + "<td></td>" * (curr_index - 2)
                            + item
                            for item in items[1:]
                        ]
                    else:
                        prepend = "<td></td>" * (curr_index - 1)
                        items = [prepend + item for item in items]
                    curr_td = items.pop()
                    append_cells = "<td></td>" * (total_cells - curr_index)
                    items = [item + append_cells for item in items]
                    saved_td.extend(items)
                    curr_index += 1
            if curr_td:
                saved_td.append(curr_td)
            return [
                _md(f"<tr>{item}</tr>", make_custom_convert_tr=True)
                for item in saved_td
            ]

        for key, value in self._tables.items():
            bsoup_table = self._BeautifulSoup(value, "html.parser")
            table_tag = bsoup_table.find("table")

            if not table_tag:
                continue

            thead = _make_thead()

            all_rows = bsoup_table.find_all("tr")
            if not all_rows:
                self._tables[key] = {"thead": thead, "rows": []}
                continue

            thead, has_thead = _handle_header_row(thead)

            if self._translate:
                self._translate_table_text(bsoup_table)

            rows = [row for row in bsoup_table.find_all(True) if row.name == "tr"]

            if not has_thead:
                rows = rows[1:]

            row_list = []
            row_index = 1
            for row in rows:
                row.insert(
                    0, self._BeautifulSoup(f"<td>{row_index}</td>", "html.parser")
                )
                markdown_row = _md(str(row), make_custom_convert_tr=True)
                if len(thead) + len(markdown_row) <= self._max_chunk_size:
                    row_list.append(f"{markdown_row}\n")
                    row_index += 1
                else:
                    row_list.extend(_handle_long_row())
                    row_index += 1
            self._tables[key] = {"thead": thead, "rows": row_list}

    def _normalize_and_clean_text(self, text: str) -> str:
        """Normalizes the text by removing extra spaces and newlines.

        Args:
            text (str): The text to be normalized.

        Returns:
            str: The normalized text.
        """
        if self._normalize_text:
            text = text.lower()
            text = re.sub(r"[^\w\s]", "", text)
            text = re.sub(r"\s+", " ", text).strip()

        if self._stopword_removal:
            text = " ".join(
                [word for word in text.split() if word not in self._stopwords]
            )

        return text

    def _process_html(self, soup: Any) -> List[Document]:
        """Processes the HTML content using BeautifulSoup and splits it using headers.

        Args:
            soup (Any): Parsed HTML content using BeautifulSoup.

        Returns:
            List[Document]: A list of Document objects containing the split content.
        """
        documents: List[Document] = []
        current_headers: Dict[str, str] = {}
        current_content: List[str] = []
        preserved_elements: Dict[str, str] = {}
        placeholder_count: int = 0

        def _get_element_text(element: Any) -> str:
            """Recursively extracts and processes the text of an element.

            Applies custom handlers where applicable, and ensures correct spacing.

            Args:
                element (Any): The HTML element to process.

            Returns:
                str: The processed text of the element.
            """
            if element.name in self._custom_handlers:
                return self._custom_handlers[element.name](element)

            if element.name in ["h1", "h2", "h3", "h4"]:
                text = element.get_text(strip=True)
                if self._translate:
                    text = translate_text(text=text)
                return f"<{element.name}>{text}</{element.name}>"

            text = ""

            if element.name is not None:
                for child in element.children:
                    child_text = _get_element_text(child).strip()
                    if text and child_text:
                        text += " "
                    text += child_text
            elif element.string:
                raw_text = element.string

                if self._translate:
                    stripped = raw_text.strip()
                    if stripped:
                        translated = translate_text(text=stripped)
                        raw_text = raw_text.replace(stripped, translated)

                text += raw_text

            text = self._normalize_and_clean_text(text)

            if not text.strip():
                return ""

            return text

        elements = soup.find_all(recursive=False)

        def _process_element(
            element: List[Any],
            documents: List[Document],
            current_headers: Dict[str, str],
            current_content: List[str],
            preserved_elements: Dict[str, str],
            placeholder_count: int,
        ) -> Tuple[List[Document], Dict[str, str], List[str], Dict[str, str], int]:
            for elem in element:
                if elem.name.lower() in ["html", "body", "div", "main"]:
                    children = elem.find_all(recursive=False)
                    (
                        documents,
                        current_headers,
                        current_content,
                        preserved_elements,
                        placeholder_count,
                    ) = _process_element(
                        children,
                        documents,
                        current_headers,
                        current_content,
                        preserved_elements,
                        placeholder_count,
                    )
                    content = " ".join(elem.find_all(string=True, recursive=False))
                    if content:
                        content = self._normalize_and_clean_text(content)
                        current_content.append(content)
                    continue

                if elem.name in [h[0] for h in self._headers_to_split_on]:
                    if current_content:
                        documents.extend(
                            self._create_documents(
                                " ".join(current_content),
                                preserved_elements,
                            )
                        )
                        current_content.clear()
                        preserved_elements.clear()
                    header_name = elem.get_text(strip=True)
                    current_headers = {
                        dict(self._headers_to_split_on)[elem.name]: header_name
                    }
                    header_html = f"<{elem.name}>{header_name}</{elem.name}>"
                    current_content.append(header_html)

                elif elem.name in self._elements_to_preserve:
                    placeholder = f"PRESERVED_{placeholder_count}"
                    preserved_elements[placeholder] = _get_element_text(elem)
                    current_content.append(placeholder)
                    placeholder_count += 1
                else:
                    content = _get_element_text(elem)
                    if content and content.strip():
                        current_content.append(content)

            return (
                documents,
                current_headers,
                current_content,
                preserved_elements,
                placeholder_count,
            )

        # Process the elements
        (
            documents,
            current_headers,
            current_content,
            preserved_elements,
            placeholder_count,
        ) = _process_element(
            elements,
            documents,
            current_headers,
            current_content,
            preserved_elements,
            placeholder_count,
        )

        # Handle any remaining content
        if current_content:
            documents.extend(
                self._create_documents(
                    " ".join(current_content),
                    preserved_elements,
                )
            )
        return documents

    def _create_documents(
        self, content: str, preserved_elements: dict[str, str]
    ) -> List[Document]:
        """Creates Document objects from the provided headers, content, and elements.

        Args:
            headers (dict): The headers to attach as metadata to the Document.
            content (str): The content of the Document.
            preserved_elements (dict): Preserved elements to be reinserted
            into the content.

        Returns:
            List[Document]: A list of Document objects.
        """
        content = re.sub(r"\s+", " ", content).strip()

        if len(content) == 0:
            return []

        content_without_metadata = re.sub(
            r"^Title:.*?\n|^Tags:.*?\n", "", content
        ).strip()
        if re.fullmatch(
            r"\s*<h([1-6])>.*?</h\1>\s*",
            content_without_metadata,
            re.IGNORECASE | re.DOTALL,
        ):
            return []

        metadata = {**self._external_metadata}
        metadata["chunk_sequence"] = self.chunk_sequence

        title = metadata.get("title", "")
        title = translate_text(text=title) if title and self._translate else title
        tags = metadata.get("prod_version", "")
        if not tags:
            tags = metadata.get("tags", "")
            if tags:
                tags = ", ".join(tags)

        metadata_prefix = ""
        if title:
            metadata_prefix += f"Title: {title}\n"
        if tags:
            metadata_prefix += f"Tags: {tags}\n"

        # del metadata["prod_version"]

        if self._table_placeholders and any(
            placeholder in content for placeholder in self._table_placeholders
        ):
            return self._further_split_chunk(
                metadata_prefix, content, metadata, preserved_elements
            )

        elif len(metadata_prefix + content) <= self._max_chunk_size:
            content = metadata_prefix + content
            page_content = self._reinsert_preserved_elements(
                content, preserved_elements
            )
            self.chunk_sequence += 1

            return [
                Document(
                    page_content=self.handle_headers(page_content), metadata=metadata
                )
            ]

        else:
            return self._further_split_chunk(
                metadata_prefix, content, metadata, preserved_elements
            )

    def _further_split_chunk(
        self,
        metadata_prefix: str,
        content: str,
        metadata: dict[Any, Any],
        preserved_elements: dict[str, str],
    ) -> List[Document]:
        """Further splits the content into smaller chunks.

        Args:
            content (str): The content to be split.
            metadata (dict): Metadata to attach to each chunk.
            preserved_elements (dict): Preserved elements
            to be reinserted into each chunk.

        Returns:
            List[Document]: A list of Document objects containing the split content.
        """
        splits = self._recursive_splitter.split_text(content)
        result = []

        for split in splits:
            metadata["chunk_sequence"] = self.chunk_sequence
            split = metadata_prefix + split
            split_with_preserved = self._reinsert_preserved_elements(
                split, preserved_elements
            )
            if split_with_preserved.strip():
                result.append(
                    Document(
                        page_content=self.handle_headers(split_with_preserved),
                        metadata=metadata,
                    )
                )
            self.chunk_sequence += 1
        return result

    def _reinsert_preserved_elements(
        self, content: str, preserved_elements: dict[str, str]
    ) -> str:
        """Reinserts preserved elements into the content into their original positions.

        Args:
            content (str): The content where placeholders need to be replaced.
            preserved_elements (dict): Preserved elements to be reinserted.

        Returns:
            str: The content with placeholders replaced by preserved elements.
        """
        for placeholder, preserved_content in preserved_elements.items():
            content = content.replace(placeholder, preserved_content.strip())
        return content
