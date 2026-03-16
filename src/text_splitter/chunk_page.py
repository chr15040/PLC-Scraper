from .html_splitter import HTMLSemanticPreservingSplitter


def chunk_page(page, external_metadata, translate):
    html_splitter = HTMLSemanticPreservingSplitter(
        headers_to_split_on=[("h1", "header 1")],
        separators=["\n\n", "\n", ". ", "! ", "? "],
        max_chunk_size=1500,
        chunk_overlap=150,
        elements_to_preserve=["table"],
        denylist_tags=["script", "style", "head"],
        external_metadata=external_metadata,
        preserve_parent_metadata=True,
        preserve_links=True,
        keep_separator="end",
        translate=translate,
    )

    try:
        html_header_splits = html_splitter.split_text(page)

        return html_header_splits

    except Exception as e:
        print(f"Failed to chunk {external_metadata['source']}: {str(e)}")
