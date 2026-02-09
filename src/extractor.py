from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source import SimpleSource


class QuoteExtractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_cache = False

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        page = args["cursor"] or 1
        return f"https://quotes.toscrape.com/page/{page}/", page

    async def fetch_rows(self, row, _type="to"):
        try:
            url, page = self.make_url(row, _type)
            res = await self.http_get(url)

            if res.status_code == 200:
                rows = []
                content = res.text
                soup = BeautifulSoup(content, "html.parser")
                
                for e in soup.find_all(class_="quote"):
                    quote_text = e.find(class_="text").get_text()
                    rows.append({
                        "uri": f'https://quotes.toscrape.com/#{sha1(quote_text)}',
                        "author": e.find(class_="author").get_text(),
                        "text": quote_text,
                        "tags": [t.get_text() for t in e.find_all(class_="tag")],
                    })

                rows = self.clamp_rows_length(rows)
                next_page = page + 1 if len(rows) > 0 else None
                
                return {
                    "rows": [
                        self.make_a_row(
                            row["uri"], self.mini_uri(r["uri"], keep_fragments=True), r
                        )
                        for r in rows
                    ],
                    "state": {
                        "pagination": {
                            _type: next_page
                        }
                    },
                }
            else:
                self.logger.error(f"Non-200 HTTP response: {res.status_code} : {url}")
                return self.make_error("HTTP", res.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))
