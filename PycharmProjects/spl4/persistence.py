import sqlite3
import atexit

import dbtools
from dbtools import Dao


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
    def __init__(self, id, description, price, quantity):
        self.id = id
        self.description = description
        self.price = price
        self.quantity = quantity


class Branche(object):
    def __init__(self, id, location, number_of_employees):
        self.id = id
        self.location = location
        self.number_of_employees = number_of_employees


class Activitie(object):
    def __init__(self, product_id, quantity, activator_id, date):
        self.product_id = product_id
        self.quantity = quantity
        self.activator_id = activator_id
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

    def execute_command_args(self, script: str, args: list) -> list:
        return self._conn.cursor().execute(script, args).fetchall()

    def insert(self, table_name, args):
        match table_name:
            case "employees":
                self._employees_dao.insert(Employee(args[0], args[1], args[2], args[3]))
            case "suppliers":
                self._suppliers_dao.insert(Supplier(args[0], args[1], args[2]))
            case "activities":
                self._activities_dao.insert(Activitie(args[0], args[1], args[2], args[3]))  # never called
            case "branches":
                self._branches_dao.insert(Branche(args[0], args[1], args[2]))
            case "products":
                self._products_dao.insert(Product(args[0], args[1], args[2], args[3]))
            case _:
                pass

    def add_activity(self, args):
        product_id = args[0]
        quantity = args[1]
        activator_id = args[2]
        date = args[3]
        if int(quantity) > 0:
            self.supply_arrival(product_id, quantity, activator_id, date)
        elif int(quantity) < 0:
            self.sales_attempt(product_id, quantity, activator_id, date)

    def supply_arrival(self, product_id, quantity, supplier_id, date):
        prod = self._products_dao.find(id=product_id)
        if prod:
            new_quantity = str(int(prod[0].quantity) + int(quantity))
            self.execute_command_args("UPDATE products SET quantity=? WHERE id=?", [new_quantity, product_id])
            self.insert("activities", [product_id, quantity, supplier_id, date])

    def sales_attempt(self, product_id, quantity, employee_id, date):
        prod = self._products_dao.find(id=product_id)
        if prod:
            new_quantity = int(prod[0].quantity) + int(quantity)
            if new_quantity >= 0:
                self.execute_command_args("UPDATE products SET quantity=? WHERE id=?", [new_quantity, product_id])
                self.insert("activities", [product_id, quantity, employee_id, date])

    def print_tables(self):
        print("Activities")
        rows = self._activities_dao.find_all_ordered("date")
        for row in rows:
            print(row)

        print("Branches")
        rows = self._branches_dao.find_all_ordered("id")
        for row in rows:
            print(row)

        print("Employees")
        rows = self._employees_dao.find_all_ordered("id")
        for row in rows:
            print(row)

        print("Products")
        rows = self._products_dao.find_all_ordered("id")
        for row in rows:
            print(row)

        print("Suppliers")
        rows = self._suppliers_dao.find_all_ordered("id")
        for row in rows:
            print(row)

    def print_employees_report(self):
        print('Employees report')
        employees = self._employees_dao.find_all_ordered_ORM("name")
        for employee in employees:
            branche = self._branches_dao.find(id=employee.branche)[0].location
            line = employee.name + " " + str(employee.salary) + " " + branche

            activities_relevent = self._activities_dao.find(activator_id=employee.id)
            tot_revenue = 0.0
            for activity in activities_relevent:
                quantity = -1 * int(activity.quantity)
                product = self._products_dao.find(id=activity.product_id)
                price = float(product[0].price)
                revenue = quantity * price
                tot_revenue += revenue

            if (tot_revenue).is_integer():
                line += " "+str(int(tot_revenue))
            else:
                line += " "+str(tot_revenue)

            print(line)

    def print_activities_report(self):
        print("Activities report")
        rows = self.execute_command("""
        SELECT activities.date, products.description, activities.quantity, employees.name, suppliers.name
          FROM activities JOIN
           products ON activities.product_id=products.id LEFT JOIN
           suppliers ON activities.activator_id=suppliers.id LEFT JOIN
           employees ON activities.activator_id=employees.id""")
        for row in rows:
            print(row)


# singleton
repo = Repository()
atexit.register(repo._close)
