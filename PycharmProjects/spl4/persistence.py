import sqlite3
import atexit
from dbtools import Dao
from dbtools import orm


# Data Transfer Objects:
class Employee(object):

    def __init__(self, id, name, salary, branche):
        self.id = id
        self.name = name
        self.salary = salary
        self.branche = branche


class Supplier(object):
    def __init__(self, id, name, contact_information):
        self.id = id
        self.name = name
        self.contact_information = contact_information


class Product(object):
    def __init__(self, id, desc, price, quan):
        self.id = id
        self.description = desc
        self.price = price
        self.quantity = quan


class Branche(object):
    def __init__(self, id, loc, num_of_emp):
        self.id = id
        self.location = loc
        self.number_of_employees = num_of_emp


class Activitie(object):
    def __init__(self, prod_id, quan, act_id, date):
        self.product_id = prod_id
        self.quantity = quan
        self.activator_id = act_id
        self.date = date


# Repository
class Repository(object):
    def __init__(self):
        self._conn = sqlite3.connect('bgumart.db')
        self._employees_dao = Dao(Employee, self._conn)
        self._suppliers_dao = Dao(Supplier, self._conn)
        self._products_dao = Dao(Product, self._conn)
        self._branches_dao = Dao(Branche, self._conn)
        self._activities_dao = Dao(Activitie, self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
            CREATE TABLE employees (
                id              INT         PRIMARY KEY,
                name            TEXT        NOT NULL,
                salary          REAL        NOT NULL,
                branche    INT REFERENCES branches(id)
            );
    
            CREATE TABLE suppliers (
                id                   INTEGER    PRIMARY KEY,
                name                 TEXT       NOT NULL,
                contact_information  TEXT
            );

            CREATE TABLE products (
                id          INTEGER PRIMARY KEY,
                description TEXT    NOT NULL,
                price       REAL NOT NULL,
                quantity    INTEGER NOT NULL
            );

            CREATE TABLE branches (
                id                  INTEGER     PRIMARY KEY,
                location            TEXT        NOT NULL,
                number_of_employees INTEGER
            );
    
            CREATE TABLE activities (
                product_id      INTEGER REFERENCES products(id),
                quantity        INTEGER NOT NULL,
                activator_id    INTEGER NOT NULL,
                date            TEXT    NOT NULL
            );
        """)

    def execute_command(self, script: str) -> list:
        return self._conn.cursor().execute(script).fetchall()

    def insert(self, table_name, args):
        match table_name:
            case "employees":
                self._employees_dao.insert(Employee(args[0], args[1], args[2], args[3]))
            case "suppliers":
                self._suppliers_dao.insert(Supplier(args[0], args[1], args[2]))
            case "activities":
                self._activities_dao.insert(Activitie(args[0], args[1], args[2], args[3]))
            case "branches":
                self._branches_dao.insert(Branche(args[0], args[1], args[2]))
            case "products":
                self._products_dao.insert(Product(args[0], args[1], args[2], args[3]))
            case _:
                print("table ",table_name,"doesent exist")


# singleton
repo = Repository()
atexit.register(repo._close)
