from config import *
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy.orm import Mapped, Session
import requests
from bs4 import BeautifulSoup
import time

engine = create_engine(STR_CONNECTION)


class Base(DeclarativeBase):
    pass


class Vacancies(Base):
    __tablename__ = TABLE_LAB2

    id: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str]
    job_description: Mapped[str]
    key_skills: Mapped[str]

    def __repr__(self):
        return (f"{self.company_name}//{self.position}//{self.job_description}//{self.key_skills}")


def one_vacancy(url):
    user_agent = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(url, headers=user_agent)
    if result.status_code == 200:
        soup = BeautifulSoup(result.content.decode(), 'lxml')

        name_vacancy = soup.find('h1')
        name_company = soup.find('a', attrs={'data-qa': 'vacancy-company-name'})
        name_descr = soup.find('div', attrs={'data-qa': 'vacancy-description'})

        lst_key = []
        name_keys = soup.find_all("span", attrs={'data-qa': 'bloko-tag__text'})
        for key in name_keys:
            lst_key.append(key.text)
        name_key_skills = ",".join(lst_key)

        return [my_decode(name_company), my_decode(name_vacancy), my_decode(name_descr), name_key_skills]


def task1():
    # url_all = 'https://izhevsk.hh.ru/vacancies/middle-python-developer'
    url_all = 'https://hh.ru/search/vacancy?text=python+middle+developer&items_on_page=10'
    user_agent = {'User-agent': 'Mozilla/5.0'}
    n_page = 0
    while n_page < 10:
        result = requests.get(url_all + '&page=' + str(n_page), headers=user_agent)
        if result.status_code == 200:

            soup = BeautifulSoup(result.content.decode(), 'lxml')
            all_href = soup.find_all("a", attrs={'data-qa': 'serp-item__title'})
            for res in all_href:
                urls = res.text, res.attrs.get('href')
                lst_res = one_vacancy(urls[1])

                vacancy = Vacancies(company_name=lst_res[0], position=lst_res[1], job_description=lst_res[2],
                                    key_skills=lst_res[3])

                with Session(engine) as session:
                    print(vacancy)
                    session.add(vacancy)
                    session.commit()
        n_page = n_page + 1


def one_vacancy_json(url):
    # user_agent = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(url)
    if result.status_code == 200:
        vacancy = result.json()

        name_vacancy = vacancy['name']
        print(name_vacancy)

        name_company = vacancy['employer']['name']
        print(name_company)

        name_descr = vacancy['description']
        print(name_descr)

        key_skills = vacancy['key_skills']
        if key_skills:
            key_skills_spis = ','.join([d['name'] for d in key_skills])
            print(key_skills_spis)
        else:
            key_skills_spis = '-'

        return [name_company, name_vacancy, name_descr, key_skills_spis]


def task2():
    url_all = 'https://api.hh.ru/vacancies?text=middle python developer&per_page=100'
    result = requests.get(url_all)
    print(result.status_code)
    print(result.json())
    vacancies = result.json().get('items')
    print(vacancies)
    for i, vacancy in enumerate(vacancies):
        print(i + 1, vacancy['name'], vacancy['url'], vacancy['alternate_url'])

        # lst_res = one_vacancy_json('https://api.hh.ru/vacancies/82987200?host=hh.ru')
        lst_res = one_vacancy_json(vacancy['url'])
        vacancy = Vacancies(company_name=lst_res[0], position=lst_res[1], job_description=lst_res[2],
                            key_skills=lst_res[3])

        with Session(engine) as session:
            print(vacancy)
            session.add(vacancy)
            session.commit()
        #time.sleep(1)


def task2_star():
    pass


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    print("Лабораторная работа №2.")

    print("Задание 1.")
    # task1()

    print("Задание 2.")
    #task2()

    # print("Задание 2 со звездочкой.")
    # task2_star()