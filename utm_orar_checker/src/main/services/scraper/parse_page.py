from bs4 import BeautifulSoup
import requests


class Parser:
    def __init__(self, page_url):
        self.page_url = page_url
        self.main_soup = self._create_main_soup()

    def _create_main_soup(self) -> BeautifulSoup:
        page = requests.get(self.page_url)
        if page.status_code == 200:
            return BeautifulSoup(page.content, "html.parser")
        raise requests.ConnectionError(f"Cannot connect to {self.page_url}, status code: {page.status_code}")

    def parse_schedule_link(self) -> tuple[str, str]:
        all_schedules_div = self.main_soup.find_all("div", {"class": "single_toggle", "role": "tablist"})
        master_schedule = all_schedules_div[-1]
        links = master_schedule.find_all('a', href=True)[1:]
        first_year, second_year = [value['href'] for value in links]
        return first_year, second_year
