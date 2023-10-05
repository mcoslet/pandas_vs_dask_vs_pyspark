from services.scraper.parse_page import Parser
from services.utils.constants import FcimUrls
from services.utils.files import check_if_schedule_is_updated

if __name__ == '__main__':
    parser = Parser(FcimUrls.SCHEDULE_URL)
    first_year, second_year = parser.parse_schedule_link()
    check_if_schedule_is_updated(first_year)
