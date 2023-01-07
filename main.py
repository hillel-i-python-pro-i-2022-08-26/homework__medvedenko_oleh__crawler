import asyncio
from asyncio.exceptions import TimeoutError

import logging

import aiohttp
from aiohttp.client_exceptions import InvalidURL, ClientConnectorError, ServerDisconnectedError, ClientOSError
from bs4 import BeautifulSoup
from urllib import parse


class MapQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self._elems = set()

    async def add_unique(self, new_elem):
        if new_elem.url not in self._elems:
            await self.queue.put(new_elem)
            self._elems.add(new_elem.url)

    async def add_to_set(self, links):
        self._elems.update(links)

    async def get_all_tasks(self):
        return [await self.queue.get() for _ in range(self.queue.qsize())]

    def write_links(self):
        with open("links_storage/parsed_links.txt", mode="w+") as file:
            file.write("\n".join([link for link in self._elems if link]))


class Config:
    def __init__(self, max_depth: int | None = None):
        self.max_depth = max_depth


class TaskRequest:
    def __init__(
            self,
            url: str,
            session: aiohttp.ClientSession,
            tasks_queue: MapQueue,
            logger: logging.Logger,
            semaphore: asyncio.Semaphore,
            current_depth: int = 0,
            base_url: str | None = None,
            config: Config = Config(),
    ):
        self.url = url
        self.session = session
        self.tasks_queue = tasks_queue
        self.semaphore = semaphore
        self.base_url = self._set_base_link(base_url)
        self.current_depth = current_depth
        self.config = config
        self.logger = logger

    def _set_base_link(self, new_link):
        if not new_link:
            parsed_new_link = parse.urlparse(self.url)
            return parse.urljoin(parsed_new_link.scheme, parsed_new_link.netloc)
        return new_link

    async def parse(self):
        self.logger.info(f"[ START ] {self.url}")
        parsed_link_text = parse.urlparse(self.url)
        if not parsed_link_text.netloc:
            try:
                self.url = await self._normalize_url()
            except ValueError as exc:
                self.logger.info(exc)
                return
        else:
            self.base_url = f"{parsed_link_text.scheme}://{parsed_link_text.netloc}"
        try:
            new_links = await self._links_extractor__wrapper()
        except RequestError as exc:
            print(exc)
            return
        self.logger.info(f"Links extracted: {len(new_links)} for url: {self.url}")
        if await self._validate_depth():
            base_task_args = {
                "url": None,
                "session": self.session,
                "tasks_queue": self.tasks_queue,
                "logger": self.logger,
                "semaphore": self.semaphore,
                "config": self.config,
                "current_depth": self.current_depth + 1,
                "base_url": self.base_url,
            }
            for link in new_links:
                if link:
                    base_task_args["url"] = link
                    await self.tasks_queue.add_unique(TaskRequest(**base_task_args))
            self.logger.info(f"[ END ] Tasks added for link {self.url}")
            return
        self.logger.info("Finished\n------------------------------")
        await self.tasks_queue.add_to_set(new_links)

    async def _links_extractor__wrapper(self):
        try:
            return await self._extract_links()
        except (
                InvalidURL,
                ClientConnectorError,
                ClientOSError,
                UnicodeDecodeError,
                AssertionError,
                ServerDisconnectedError,
                TimeoutError,
        ) as exc:
            raise RequestError(exc) from exc

    async def _normalize_url(self):
        self.logger.info(f"Normalizing url for {self.url}")
        if not self.base_url:
            raise ValueError("Can't find base link for normalizing")
        return parse.urljoin(self.base_url, self.url)

    async def _validate_depth(self):
        self.logger.info(f"Validating depth for {self.url}")
        if self.config.max_depth:
            return self.current_depth != self.config.max_depth
        return True

    async def _extract_links(self):
        html = await self._get_html_page()
        self.logger.info(f"Extracting links for url: {self.url}")
        soup = BeautifulSoup(html, features="lxml")
        return set(map(lambda anchor: anchor.get("href"), soup.findAll("a")))

    async def _get_html_page(self):
        async with self.semaphore:
            async with self.session.get(self.url) as response:
                self.logger.info(f"Making request for url: {self.url}")
                return await response.text()


class RequestError(Exception):
    def __init__(self, message):
        self.message = message


def init_logger():
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)
    return logger


async def main():
    links = [
        "https://www.linkedin.com/",
        "https://github.com/"
    ]
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0",
    }
    queue_obj = MapQueue()
    config = Config(max_depth=2)
    semaphore = asyncio.Semaphore(20)
    logger = init_logger()
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(headers=header, timeout=timeout) as session:
        await asyncio.gather(
            *[
                TaskRequest(url, session, queue_obj, logger, config=config, semaphore=semaphore).parse()
                for url in links
            ],
        )
        while not queue_obj.queue.empty():
            new_tasks = await queue_obj.get_all_tasks()
            await asyncio.gather(*[task.parse() for task in new_tasks])
    queue_obj.write_links()


if __name__ == "__main__":
    asyncio.run(main())
