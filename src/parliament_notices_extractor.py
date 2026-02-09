from bs4 import BeautifulSoup
from nthrow.source import SimpleSource
import re


class ParliamentNoticesExtractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_cache = False

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        page = args["cursor"] or 1
        return f"https://hr.parliament.gov.np/en/parliamentary-notices?get_by=all&n_type=parliament_notices&page={page}", page

    async def fetch_rows(self, row, _type="to"):
        try:
            url, page = self.make_url(row, _type)
            res = await self.http_get(url, verify=False)

            if res.status_code == 200:
                rows = []
                soup = BeautifulSoup(res.text, "html.parser")
                
                table = soup.find('table')
                if not table:
                    return {
                        "rows": [],
                        "state": {"pagination": {_type: None}}
                    }
                
                table_rows = table.find_all('tr')[1:]
                for tr in table_rows:
                    cols = tr.find_all('td')
                    if len(cols) < 2:
                        continue
                    
                    title = cols[1].get_text(strip=True)
                    link = tr.find('a', href=True)
                    
                    if not link or '/notices/' not in link['href']:
                        continue
                    
                    if 'notice' not in title.lower():
                        continue
                    
                    href = link['href']
                    notice_url = href if href.startswith('http') else f"https://hr.parliament.gov.np{href}"
                    
                    bs_date_match = re.search(r'(\d{4}-\d{2}-\d{2})', title)
                    bs_date = bs_date_match.group(1) if bs_date_match else None
                    
                    if not bs_date or bs_date < "2079-10-09":
                        continue
                    
                    rows.append({
                        "uri": notice_url,
                        "title": title,
                        "bs_date": bs_date,
                        "notice_url": notice_url
                    })
                
                rows = self.clamp_rows_length(rows)
                next_page = page + 1 if len(rows) > 0 else None
                
                return {
                    "rows": [
                        self.make_a_row(row["uri"], self.mini_uri(r["uri"]), r)
                        for r in rows
                    ],
                    "state": {"pagination": {_type: next_page}}
                }
            else:
                self.logger.error(f"Non-200 HTTP response: {res.status_code} : {url}")
                return self.make_error("HTTP", res.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))
