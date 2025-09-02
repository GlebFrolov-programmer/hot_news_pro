from typing import Dict, Any


class NewsItem:
    def __init__(
            self,
            url: str,
            title: str,
            source: str,
            metadata: Dict[str, Any],
            approved: bool,
            raw_data: str = ''
    ):
        self.source = source
        self.metadata = metadata
        self.url = url
        self.title = title
        self.raw_data = raw_data
        self.approved = approved

    def __repr__(self):
        return (f"NewsItem(source={self.source!r}, metadata={self.metadata!r}, url={self.url!r}, "
                f"title={self.title!r}, raw_data={self.raw_data!r}, approved={self.approved!r})")

    def get_full_data_dict(self) -> dict:
        return {
            "source": self.source,
            "metadata": self.metadata,
            "url": self.url,
            "title": self.title,
            "raw_data": self.raw_data,
            "approved": self.approved,
        }
