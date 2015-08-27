import operator

import momoko
import peewee
from tornado import gen


class PostgresqlAsyncDatabase(peewee.PostgresqlDatabase):
    def _connect(self, database, encoding=None, **kwargs):
        dsn_params = ['dbname=%s' % database]
        if 'user' in kwargs:
            dsn_params.append('user=%s' % kwargs.pop('user'))
        if 'password' in kwargs:
            if kwargs['password']:
                dsn_params.append('password=%s' % kwargs.pop('password'))
            else:
                kwargs.pop('password')
        if 'host' in kwargs:
            dsn_params.append('host=%s' % kwargs.pop('host'))
        if 'port' in kwargs:
            dsn_params.append('port=%s' % kwargs.pop('port', 5432))
        dsn_params = ' '.join(dsn_params)
        conn = momoko.Pool(
            dsn=dsn_params,
            setsession=('SET TIME ZONE UTC',),
            **kwargs)
        return conn

    @gen.coroutine
    def execute_sql(self, sql, params=None, require_commit=True):
        params = params or ()
        peewee.logger.debug((sql, params))

        with self.exception_wrapper():
            conn = self.get_conn()
            try:
                if require_commit and self.get_autocommit():
                    cursors = yield conn.transaction(((sql, params),))
                    for cursor in cursors:
                        pass
                else:
                    cursor = yield conn.execute(sql, params)
            except Exception as exc:
                if self.sql_error_handler(exc, sql, params, require_commit):
                    raise
        raise gen.Return(cursor)

    @gen.coroutine
    def last_insert_id(self, cursor, model):
        sequence = self._get_pk_sequence(model)
        if not sequence:
            return

        meta = model._meta
        if meta.schema:
            schema = '%s.' % meta.schema
        else:
            schema = ''

        yield cursor.execute("SELECT CURRVAL('%s\"%s\"')" % (schema, sequence))
        result = cursor.fetchone()[0]
        if self.get_autocommit():
            self.commit()
        raise gen.Return(result)

    @gen.coroutine
    def get_tables(self, schema='public'):
        query = ('SELECT tablename FROM pg_catalog.pg_tables '
                 'WHERE schemaname = %s ORDER BY tablename')
        cursor = yield self.execute_sql(query, (schema,))
        result = [r for r, in cursor.fetchall()]
        raise gen.Return(result)

    @gen.coroutine
    def get_indexes(self, table, schema='public'):
        query = """
            SELECT
                i.relname, idxs.indexdef, idx.indisunique,
                array_to_string(array_agg(cols.attname), ',')
            FROM pg_catalog.pg_class AS t
            INNER JOIN pg_catalog.pg_index AS idx ON t.oid = idx.indrelid
            INNER JOIN pg_catalog.pg_class AS i ON idx.indexrelid = i.oid
            INNER JOIN pg_catalog.pg_indexes AS idxs ON
                (idxs.tablename = t.relname AND idxs.indexname = i.relname)
            LEFT OUTER JOIN pg_catalog.pg_attribute AS cols ON
                (cols.attrelid = t.oid AND cols.attnum = ANY(idx.indkey))
            WHERE t.relname = %s AND t.relkind = %s AND idxs.schemaname = %s
            GROUP BY i.relname, idxs.indexdef, idx.indisunique
            ORDER BY idx.indisunique DESC, i.relname;"""
        cursor = yield self.execute_sql(query, (table, 'r', schema))
        result = [peewee.IndexMetadata(row[0], row[1], row[3].split(','),
                                       row[2], table)
                  for row in cursor.fetchall()]
        raise gen.Return(result)

    @gen.coroutine
    def get_columns(self, table, schema='public'):
        query = """
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position"""
        cursor = yield self.execute_sql(query, (table, schema))
        pks = yield self.get_primary_keys(table, schema)
        pks = set(pks)
        result = [peewee.ColumnMetadata(name, dt, null == 'YES', name in pks,
                                        table)
                  for name, null, dt in cursor.fetchall()]
        raise gen.Return(result)

    @gen.coroutine
    def get_primary_keys(self, table, schema='public'):
        query = """
            SELECT kc.column_name
            FROM information_schema.table_constraints AS tc
            INNER JOIN information_schema.key_column_usage AS kc ON (
                tc.table_name = kc.table_name AND
                tc.table_schema = kc.table_schema AND
                tc.constraint_name = kc.constraint_name)
            WHERE
                tc.constraint_type = %s AND
                tc.table_name = %s AND
                tc.table_schema = %s"""
        cursor = yield self.execute_sql(query, ('PRIMARY KEY', table, schema))
        result = [row for row, in cursor.fetchall()]
        raise gen.Return(result)

    @gen.coroutine
    def get_foreign_keys(self, table, schema='public'):
        sql = """
            SELECT
                kcu.column_name, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON (tc.constraint_name = kcu.constraint_name AND
                    tc.constraint_schema = kcu.constraint_schema)
            JOIN information_schema.constraint_column_usage AS ccu
                ON (ccu.constraint_name = tc.constraint_name AND
                    ccu.constraint_schema = tc.constraint_schema)
            WHERE
                tc.constraint_type = 'FOREIGN KEY' AND
                tc.table_name = %s AND
                tc.table_schema = %s"""
        cursor = yield self.execute_sql(sql, (table, schema))
        result = [peewee.ForeignKeyMetadata(row[0], row[1], row[2], table)
                  for row in cursor.fetchall()]
        raise gen.Return(result)

    @gen.coroutine
    def sequence_exists(self, sequence):
        res = yield self.execute_sql("""
            SELECT COUNT(*) FROM pg_class, pg_namespace
            WHERE relkind='S'
                AND pg_class.relnamespace = pg_namespace.oid
                AND relname=%s""", (sequence,))
        raise gen.Return(bool(res.fetchone()[0]))

    @gen.coroutine
    def set_search_path(self, *search_path):
        path_params = ','.join(['%s'] * len(search_path))
        yield self.execute_sql(
            'SET search_path TO %s' % path_params, search_path)


class AsyncUpdateQuery(peewee.UpdateQuery):
    @gen.coroutine
    def execute(self):
        cursor = yield self._execute()
        raise gen.Return(self.database.rows_affected(cursor))


class AsyncDeleteQuery(peewee.DeleteQuery):
    @gen.coroutine
    def execute(self):
        cursor = yield self._execute()
        raise gen.Return(self.database.rows_affected(cursor))


class AsyncInsertQuery(peewee.InsertQuery):
    @gen.coroutine
    def execute(self):
        insert_with_loop = all((
            self._is_multi_row_insert,
            self._query is None,
            not self.database.insert_many))
        if insert_with_loop:
            raise gen.Return(self._insert_with_loop())

        cursor = yield self._execute()
        if not self._is_multi_row_insert:
            if self.database.insert_returning:
                pk_row = cursor.fetchone()
                meta = self.model_class._meta
                clean_data = [
                    field.python_value(column)
                    for field, column
                    in zip(meta.get_primary_key_fields(), pk_row)]
                if self.model_class._meta.composite_key:
                    raise gen.Return(clean_data)
                raise gen.Return(clean_data[0])
            else:
                raise gen.Return(self.database.last_insert_id(
                    cursor, self.model_class))
        elif self._return_id_list:
            raise gen.Return(map(operator.itemgetter(0), cursor.fetchall()))
        else:
            raise gen.Return(True)


class AsyncRawQuery(peewee.RawQuery):
    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = self.tuples().first()
        else:
            cursor = yield self._execute()
            row = cursor.fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            gen.Return(row)

    @gen.coroutine
    def execute(self):
        if self._qr is None:
            if self._tuples:
                ResultWrapper = peewee.TuplesQueryResultWrapper
            elif self._dicts:
                ResultWrapper = peewee.DictQueryResultWrapper
            else:
                ResultWrapper = peewee.NaiveQueryResultWrapper
            cursor = yield self._execute()
            self._qr = ResultWrapper(self.model_class, cursor, None)
        raise gen.Return(self._qr)


class AsyncSelectQuery(peewee.SelectQuery):
    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = self.tuples().first()
        else:
            cursor = yield self._execute()
            row = cursor.fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            gen.Return(row)

    @gen.coroutine
    def execute(self):
        if self._dirty or not self._qr:
            model_class = self.model_class
            query_meta = self.get_query_meta()
            ResultWrapper = self._get_result_wrapper()
            cursor = yield self._execute()
            self._qr = ResultWrapper(model_class, cursor, query_meta)
            self._dirty = False
            raise gen.Return(self._qr)
        else:
            raise gen.Return(self._qr)

    @gen.coroutine
    def exists(self):
        clone = self.paginate(1, 1)
        clone._select = [peewee.SQL('1')]
        cursor = yield clone.scalar()
        raise gen.Return(bool(cursor))

    @gen.coroutine
    def get(self):
        clone = self.paginate(1, 1)
        try:
            cursor = yield clone.execute()
            raise gen.Return(cursor.next())
        except StopIteration:
            raise self.model_class.DoesNotExist(
                'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
                % self.sql())

    @gen.coroutine
    def first(self):
        res = yield self.execute()
        res.fill_cache(1)
        try:
            raise gen.Return(res._result_cache[0])
        except IndexError:
            pass


class AsyncModel(peewee.Model):
    @classmethod
    def select(cls, *selection):
        query = AsyncSelectQuery(cls, *selection)
        if cls._meta.order_by:
            query = query.order_by(*cls._meta.order_by)
        return query

    @classmethod
    def update(cls, **update):
        fdict = dict((cls._meta.fields[f], v) for f, v in update.items())
        return AsyncUpdateQuery(cls, fdict)

    @classmethod
    def insert(cls, **insert):
        return AsyncInsertQuery(cls, insert)

    @classmethod
    def insert_many(cls, rows):
        return AsyncInsertQuery(cls, rows=rows)

    @classmethod
    def insert_from(cls, fields, query):
        return AsyncInsertQuery(cls, fields=fields, query=query)

    @classmethod
    def delete(cls):
        return AsyncDeleteQuery(cls)

    @classmethod
    def raw(cls, sql, *params):
        return AsyncRawQuery(cls, sql, *params)

    @classmethod
    @gen.coroutine
    def create(cls, **query):
        inst = cls(**query)
        yield inst.save(force_insert=True)
        inst._prepare_instance()
        raise gen.Return(inst)

    @classmethod
    @gen.coroutine
    def get_or_create(cls, **kwargs):
        defaults = kwargs.pop('defaults', {})
        sq = cls.select().filter(**kwargs)
        try:
            cursor = yield sq.get()
            raise gen.Return((cursor, False))
        except cls.DoesNotExist:
            try:
                params = dict((k, v) for k, v in kwargs.items()
                              if '__' not in k)
                params.update(defaults)
                with cls._meta.database.atomic():
                    cursor = yield cls.create(**params), True
                    raise gen.Return((cursor, True))
            except peewee.IntegrityError as exc:
                try:
                    cursor = yield sq.get()
                    raise gen.Return((cursor, False))
                except cls.DoesNotExist:
                    raise exc

    @classmethod
    @gen.coroutine
    def table_exists(cls):
        kwargs = {}
        if cls._meta.schema:
            kwargs['schema'] = cls._meta.schema
        tables = yield cls._meta.database.get_tables(**kwargs)
        raise gen.Return(cls._meta.db_table in tables)

    @classmethod
    @gen.coroutine
    def create_table(cls, fail_silently=False):
        table_exists = yield cls.table_exists()
        if fail_silently and table_exists:
            return

        db = cls._meta.database
        pk = cls._meta.primary_key
        if db.sequences and pk.sequence:
            sequence_exists = yield db.sequence_exists(pk.sequence)
            if not sequence_exists:
                yield db.create_sequence(pk.sequence)

        yield db.create_table(cls)
        yield cls._create_indexes()

    @classmethod
    @gen.coroutine
    def _create_indexes(cls):
        db = cls._meta.database
        for field in cls._fields_to_index():
            yield db.create_index(cls, [field], field.unique)

        if cls._meta.indexes:
            for fields, unique in cls._meta.indexes:
                yield db.create_index(cls, fields, unique)

    @gen.coroutine
    def save(self, force_insert=False, only=None):
        field_dict = dict(self._data)
        pk_field = self._meta.primary_key
        pk_value = self._get_pk_value()
        if only:
            field_dict = self._prune_fields(field_dict, only)
        if pk_value is not None and not force_insert:
            if self._meta.composite_key:
                for pk_part_name in pk_field.field_names:
                    field_dict.pop(pk_part_name, None)
            else:
                field_dict.pop(pk_field.name, None)
            rows = self.update(**field_dict).where(self._pk_expr())
            rows = yield rows.execute()
        else:
            pk_from_cursor = yield self.insert(**field_dict).execute()
            if pk_from_cursor is not None:
                pk_value = pk_from_cursor
            self._set_pk_value(pk_value)
            rows = 1
        self._dirty.clear()
        raise gen.Return(rows)

    @gen.coroutine
    def delete_instance(self, recursive=False, delete_nullable=False):
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                model = fk.model_class
                if fk.null and not delete_nullable:
                    q = model.update(**{fk.name: None}).where(query)
                    yield q.execute()
                else:
                    yield model.delete().where(query).execute()
        cursor = yield self.delete().where(self._pk_expr()).execute()
        raise gen.Return(cursor)
