import json
from .sql_client import DBClient, Users, UsersSchema, Tokens, \
    TokensSchema, SearchParams, SearchParamsSchema


class PostgresAPI:
    def __init__(self, db_config: dict):
        """Данный класс предназначен для работы с запросами
        к базе данных PostgreSQL с использованием библиотеки sqlalchemy."""
        self.client = DBClient(**db_config)
        self.client.get_connection()

        self.engine = self.client.engine
        self.client.create_tables()

    def check_token(self, vk_id: int) -> dict:
        """Метод для поиска зарегистрированных пользователей в
        базе данных tokens для нахождения токена по id
        пользователя ВКонтакте."""
        result = self.client.session.query(Tokens).filter_by(vk_id=vk_id).first()
        if result:
            dumped_data = TokensSchema().dump(result)
            return dumped_data

    def add_new_user(self, user_data: dict) -> dict:
        """Добавляет новую запись с информацией о пользователе приложения
        для дальнейшей обработке статистики пользования приложения.
        Дополнительно производится проверка наличия информации о
        пользователе приложения по id ВКонтакте."""
        result = self.get_user(user_data['vk_id'])
        if result:
            return result
        else:
            user = Users(**user_data)
            self.client.session.add(user)
            return self.get_user(user_data['vk_id'])

    def get_user(self, vk_id: int) -> dict:
        """Получение информации о пользователе по id пользователя ВКонтакте."""
        result = self.client.session.query(Users).filter_by(
            vk_id=vk_id
        ).first()
        dumped_data = UsersSchema().dump(result)
        return dumped_data

    def add_params(self, param_data: dict) -> dict:
        """Добавляет параметры поиска пользователя в таблицу search_params.
        Аналогичнометоду add_new_user проверяет был ли добавлен данный
        пользователь раньше по id ВКонтакте."""
        result = self.get_params(param_data['vk_id'])
        if result:
            return result
        else:
            params = SearchParams(**param_data)
            self.client.session.add(params)
            return self.get_params(param_data['vk_id'])

    def delete_user(self, vk_id: int):
        """Удаление пользователя из таблицы users по id пользователя
         ВКонтакте."""
        self.client.session.query(Users).filter_by(vk_id=vk_id).delete()

    def delete_params(self, vk_id: int):
        """Удаление данных о параметрах поиска конкретного пользователя
        по его id в ВКонтакте."""
        self.client.session.query(SearchParams).filter_by(
            vk_id=vk_id).delete()

    def get_params(self, vk_id: int) -> dict:
        """Получение информации о параметрах поиска пользователя по
        его id ВКонтакте."""
        result = self.client.session.query(SearchParams).filter_by(
            vk_id=vk_id).first()
        dumped_data = SearchParamsSchema().dump(result)
        return dumped_data


if __name__ == '__main__':
    with open('sql_config.json', 'r') as f:
        configs = json.load(f)
    db_api = PostgresAPI(db_config=configs)
    db_api.client.rebuild_database()
    db_api.client.create_tables()
