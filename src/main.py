#!/usr/bin/env python3

import argparse
import json
import pymysql
import sys
import os
from typing import List, Dict, Any
from datetime import date

class DBConnector:
    def __init__(self, host: str, user: str, password: str, db: str, port: int = 3306):
        self.cfg = dict(host=host, user=user, password=password, database=db, port=port,
                        cursorclass=pymysql.cursors.DictCursor, autocommit=False)
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = pymysql.connect(**self.cfg)
        return self.conn

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None

class SchemaManager:
    CREATE_ROOMS = """
    CREATE TABLE IF NOT EXISTS rooms (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    ) ENGINE=InnoDB;
    """

    CREATE_STUDENTS = """
    CREATE TABLE IF NOT EXISTS students (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR(255),
        birthday DATE NOT NULL,
        room INT,
        sex VARCHAR(20),
        CONSTRAINT fk_students_room FOREIGN KEY (room) REFERENCES rooms (id) ON DELETE SET NULL
    ) ENGINE=InnoDB;
    """

    INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS idx_students_room ON students (room);",
        "CREATE INDEX IF NOT EXISTS idx_students_birthday ON students (birthday);",
        "CREATE INDEX IF NOT EXISTS idx_students_sex ON students (sex);",
        "CREATE INDEX IF NOT EXISTS idx_students_room_birthday ON students (room, birthday);",
    ]

    def __init__(self, conn):
        self.conn = conn

    def create_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(self.CREATE_ROOMS)
            cur.execute(self.CREATE_STUDENTS)
        self.conn.commit()

    def create_indexes(self):
        with self.conn.cursor() as cur:
            for sql in self.INDEXES_SQL:
                try:
                    cur.execute(sql)
                except Exception:
                    try:
                        alt = sql.replace('CREATE INDEX IF NOT EXISTS ', 'CREATE INDEX ')
                        cur.execute(alt)
                    except Exception:
                        pass
        self.conn.commit()

class DataLoader:
    def __init__(self, conn):
        self.conn = conn

    def upsert_rooms(self, rooms: List[Dict[str, Any]]):
        sql = "INSERT INTO rooms (id, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name)"
        with self.conn.cursor() as cur:
            cur.executemany(sql, [(r['id'], r.get('name')) for r in rooms])
        self.conn.commit()

    def upsert_students(self, students: List[Dict[str, Any]]):
        sql = ("INSERT INTO students (id, name, birthday, room, sex) VALUES (%s, %s, %s, %s, %s) "
               "ON DUPLICATE KEY UPDATE name=VALUES(name), birthday=VALUES(birthday), room=VALUES(room), sex=VALUES(sex)")
        params = []
        for s in students:
            b = s.get('birthday')
            params.append((s['id'], s.get('name'), b, s.get('room'), s.get('sex')))
        with self.conn.cursor() as cur:
            cur.executemany(sql, params)
        self.conn.commit()

class QueriesRunner:
    def __init__(self, conn):
        self.conn = conn

    def rooms_with_counts(self) -> List[Dict[str, Any]]:
        sql = """
        SELECT r.id, r.name, COUNT(s.id) AS student_count
        FROM rooms r
        LEFT JOIN students s ON s.room = r.id
        GROUP BY r.id, r.name
        ORDER BY r.id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def five_smallest_avg_age(self) -> List[Dict[str, Any]]:
        sql = """
        SELECT r.id, r.name, AVG(TIMESTAMPDIFF(YEAR, s.birthday, CURDATE())) AS avg_age, COUNT(s.id) AS cnt
        FROM rooms r
        JOIN students s ON s.room = r.id
        GROUP BY r.id, r.name
        HAVING cnt > 0
        ORDER BY avg_age ASC
        LIMIT 5
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def five_largest_age_diff(self) -> List[Dict[str, Any]]:
        sql = """
        SELECT r.id, r.name,
               (MAX(TIMESTAMPDIFF(YEAR, s.birthday, CURDATE())) - MIN(TIMESTAMPDIFF(YEAR, s.birthday, CURDATE()))) AS age_diff,
               COUNT(s.id) AS cnt
        FROM rooms r
        JOIN students s ON s.room = r.id
        GROUP BY r.id, r.name
        HAVING cnt > 1
        ORDER BY age_diff DESC
        LIMIT 5
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def rooms_with_mixed_sex(self) -> List[Dict[str, Any]]:
        sql = """
        SELECT r.id, r.name, GROUP_CONCAT(DISTINCT s.sex) AS sexes, COUNT(DISTINCT s.sex) AS distinct_sex_count
        FROM rooms r
        JOIN students s ON s.room = r.id
        GROUP BY r.id, r.name
        HAVING distinct_sex_count > 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

class Formatter:
    @staticmethod
    def to_json(data: Dict[str, Any]) -> str:
        return json.dumps(data, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _dict_to_xml(tag: str, d: Dict[str, Any]) -> str:
        parts = [f'<{tag}>']
        for k, v in d.items():
            if isinstance(v, list):
                parts.append(f'<{k}>')
                for item in v:
                    parts.append(Formatter._dict_to_xml('item', item))
                parts.append(f'</{k}>')
            else:
                parts.append(f'<{k}>{Formatter._escape_xml(v)}</{k}>')
        parts.append(f'</{tag}>')
        return ''.join(parts)

    @staticmethod
    def _escape_xml(value: Any) -> str:
        if value is None:
            return ''
        s = str(value)
        return (s.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&apos;'))

    @staticmethod
    def to_xml(root_tag: str, payload: Dict[str, Any]) -> str:
        xml = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml.append(Formatter._dict_to_xml(root_tag, payload))
        return '\n'.join(xml)


def parse_args():
    p = argparse.ArgumentParser(description='Load rooms and students into MySQL and run queries')
    p.add_argument('--students', required=True, help='Path to students.json')
    p.add_argument('--rooms', required=True, help='Path to rooms.json')
    p.add_argument('--format', choices=['json', 'xml'], default='json', help='Output format')
    p.add_argument('--db-host', default='localhost')
    p.add_argument('--db-user', default='root')
    p.add_argument('--db-password', default='')
    p.add_argument('--db-name', default='bookstore_db')
    p.add_argument('--db-port', type=int, default=3306)
    return p.parse_args()


def load_json_file(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    args = parse_args()

    if not os.path.exists(args.students):
        print('Students file not found:', args.students, file=sys.stderr)
        sys.exit(2)
    if not os.path.exists(args.rooms):
        print('Rooms file not found:', args.rooms, file=sys.stderr)
        sys.exit(2)

    students = load_json_file(args.students)
    rooms = load_json_file(args.rooms)

    db = DBConnector(args.db_host, args.db_user, args.db_password, args.db_name, port=args.db_port)
    try:
        conn = db.connect()
    except Exception as e:
        print('Failed to connect to DB:', e, file=sys.stderr)
        sys.exit(3)

    try:
        schema = SchemaManager(conn)
        schema.create_schema()
        schema.create_indexes()

        loader = DataLoader(conn)
        loader.upsert_rooms(rooms)
        loader.upsert_students(students)

        runner = QueriesRunner(conn)
        res = {
            'rooms_counts': runner.rooms_with_counts(),
            'five_smallest_avg_age': runner.five_smallest_avg_age(),
            'five_largest_age_diff': runner.five_largest_age_diff(),
            'rooms_with_mixed_sex': runner.rooms_with_mixed_sex()
        }

        if args.format == 'json':
            out = Formatter.to_json(res)
        else:
            out = Formatter.to_xml('results', res)

        print(out)

    finally:
        db.close()


if __name__ == '__main__':
    main()
