\echo Use "CREATE EXTENSION ..." to load this file! \quit

create schema integrations;

CREATE TABLE IF NOT EXISTS integrations.dataobject
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    name text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" DEFAULT ''::text,
    sourcedefinition text COLLATE pg_catalog."default",
    sourcename text COLLATE pg_catalog."default",
    destinationname text COLLATE pg_catalog."default" NOT NULL,
    mergeinsert boolean NOT NULL DEFAULT true,
    mergeupdate boolean NOT NULL DEFAULT true,
    mergedelete boolean NOT NULL DEFAULT true,
    logchanges boolean NOT NULL DEFAULT true,
    CONSTRAINT dataobject_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integrations.dataattributes
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    dataobject_id integer NOT NULL,
    sourcename text COLLATE pg_catalog."default" NOT NULL,
    destinationname text COLLATE pg_catalog."default",
    isunique boolean NOT NULL DEFAULT false,
    ignorecompare boolean NOT NULL DEFAULT false,
    istimestamp boolean NOT NULL DEFAULT false,
    description text COLLATE pg_catalog."default" DEFAULT ''::text,
    CONSTRAINT dataattributes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integrations.log
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
    "timestamp" timestamp with time zone DEFAULT clock_timestamp(),
    dataobject_id integer NOT NULL,
    command text COLLATE pg_catalog."default",
    iserror boolean,
    CONSTRAINT log_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integrations.logdetails
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
    log_id bigint,
    details jsonb storage extended compression lz4 NOT NULL,
    CONSTRAINT logdetails_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE VIEW integrations.v_log
 AS
 SELECT l.id,
    l."timestamp",
    l.dataobject_id,
    l.command,
    case when l.IsError then 1 else 0 end as isError,
	jsonb_array_length(ld.details->'INSERT') AS inserted,
	jsonb_array_length(ld.details->'DELETE') AS deleted,
	jsonb_array_length(ld.details->'UPDATE') AS updated
FROM integrations.log l
LEFT JOIN integrations.logdetails ld ON l.id = ld.log_id;
	 
CREATE OR REPLACE FUNCTION integrations.usp_generate(
	in_dataobjectid integer,
	in_useaction boolean DEFAULT false)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	_sql text;

	/* METADATA */
	_mergeInsert bool;
	_mergeUpdate bool;
	_mergeDelete bool;
	_logChanges bool;
	
	_srcDOName text = '';
	_srcFields text = '';
	_srcFieldsUnique text = '';
	_srcFieldsTimestamp text = '';
	_srcFieldsTimestampFilter text = '';
	_dstDOName text = '';
	_dstFields text = '';
	_srcInsertingFields text = '';
	_dstReturningFields text = '';
	_targetFieldsTimestampFilter text = '';
	_mergeOn text = '';
	_updateFields text = '';
	_resultFieldsDefinition text = '';

	_compareFields text = '';
	_sourceDefinition text = '';

begin
/*
Генерация sql-кода для запроса данных из источника и их загрузки на сторону приемника с помощью MERGE.
Возможности генерации:
- можно записать в колонку DataObject.Action произвольные CTE-запросы для [source] и [target]
- при вызове через sp_executesql параметры @StartDatetime, @EndDateTime можно использовать в запросах DataObject.Action, чтобы
	автоматически применять фильтрация по времени
*/
-- select integrations.usp_generate(11,1)

	if in_dataObjectId is null or not exists (select id from integrations.DataObject where id = in_dataObjectId) then
		raise notice '%,%','Не указан или не найден id объекта данных!', 'in_dataObjectId ='||coalesce(in_dataObjectId::text,'NULL');
		return null;
	end if;

	if in_useAction is null then in_useAction = false; end if;

	select MergeInsert, MergeUpdate, MergeDelete, LogChanges
	into _mergeInsert,_mergeUpdate,_mergeDelete,_logChanges 
	from integrations.DataObject 
	where DataObject.id = in_dataObjectId;

	select SourceName, DestinationName
	into _srcDOName, _dstDOName
	from integrations.DataObject 
	where DataObject.id = in_dataObjectId;

	_sql = concat('
	drop table if exists tempTest',pg_backend_pid(),';
	create temp table tempTest',pg_backend_pid(),' on commit drop as
		select * from ',_dstDOName,' limit 0;
	');
	execute(_sql);
	
	_sql = concat('
	drop table if exists tempMetadata',pg_backend_pid(),';
	create temp table tempMetadata',pg_backend_pid(),' on commit drop as
		select 
			column_name as name, 
			data_type as system_type_name,
			case when data_type in (''text'',''bytea'',''character varying'',''json'',''jsonb'') and character_maximum_length is null then -1 else character_maximum_length end as max_length
		from information_schema.columns where table_name = ''temptest',pg_backend_pid(),'''
	;
	');
	execute(_sql);
	
	_sql = concat('
	select 
		string_agg(SourceName,E'',''),
		string_agg(coalesce(DestinationName,SourceName),E'',''),
		string_agg(''source.'' || SourceName, E'',''),
		string_agg(''target.'' || coalesce(DestinationName,SourceName), E'',''),
		string_agg(concat(coalesce(DestinationName,SourceName),''=source.'',SourceName),E'',''),
		string_agg(concat(concat(coalesce(DestinationName,SourceName)),'' '', system_type_name),E'','')
	from integrations.DataAttributes 
	join (select * from tempMetadata',pg_backend_pid(),') as metaData 
		on lower(coalesce(DataAttributes.DestinationName,DataAttributes.SourceName)) = lower(metaData.name)
	where DataAttributes.DataObject_id = ',in_dataObjectId,';
	');
	execute (_sql) into _srcFields,_dstFields,_srcInsertingFields,_dstReturningFields,_updateFields,_resultFieldsDefinition;

	_sql = 'drop table if exists tempTest'||pg_backend_pid()||'; drop table if exists tempMetadata'||pg_backend_pid()||';';
	execute(_sql);
	
	IF _srcFields is null or length(_srcFields) = 0 then 
		raise notice '%,%','У объекта данных нет атрибутов для интеграции!',_sql; 
		return null; 
	end if;
		
	SELECT
		string_agg(SourceName,',')
	into _srcFieldsUnique
	FROM integrations.DataAttributes 
	WHERE DataAttributes.DataObject_id = in_dataObjectId and IsUnique = true;

	select 
		string_agg(SourceName || ' DESC',','),
		string_agg('(' || SourceName || ' >= _startDateTime and ' || SourceName || ' <= _endDateTime)',' OR '),
		string_agg('(target.' || coalesce(DestinationName, SourceName) || ' >= _startDateTime and target.' || coalesce(DestinationName, SourceName) || ' <= _endDateTime)',' OR ')
	into _srcFieldsTimestamp,_srcFieldsTimestampFilter,_targetFieldsTimestampFilter
	from integrations.DataAttributes 	
	where DataAttributes.DataObject_id = in_dataObjectId and IsTimestamp = true;

	if length(coalesce(_srcFieldsTimestampFilter,'')) = 0 then _srcFieldsTimestampFilter = '1=1'; end if;
	if length(coalesce(_targetFieldsTimestampFilter,'')) = 0 then _targetFieldsTimestampFilter = '1=1'; end if;

	SELECT 
		string_agg(concat('target.',coalesce(DestinationName,SourceName),' = source.',SourceName),E' AND ')
	into _mergeOn
	from integrations.DataAttributes 
	where DataAttributes.DataObject_id = in_dataObjectId and IgnoreCompare = false and IsUnique = true;

	if _mergeOn is null or length(_mergeOn) = 0 then 
		raise notice '%','У объекта данных нет атрибутов для сравнения в MERGE ON!'; 
		return null; 
	end if;
	
	/* MERGE QUERY */

	SELECT 
		 string_agg(concat('target.',coalesce(DestinationName,SourceName),' is distinct from source.',SourceName),E' OR ')
	into _compareFields
	from integrations.DataAttributes 
	where DataAttributes.DataObject_id = in_dataObjectId and IgnoreCompare = false and IsUnique = false;
	
	if _compareFields is null or length(_compareFields)=0 then 
		_compareFields = '1<>1';
	end if;
	
	if in_useAction = true then
		select coalesce(sourceDefinition,'') 
		into _sourceDefinition
		from integrations.DataObject 
		where DataObject.id = in_dataObjectId;
	end if;
	
	_sql = '';

	IF length(_sourceDefinition) > 0 then 
		_sql = _sql || _sourceDefinition;
	end if;
	
	IF length(_srcFieldsUnique) = 0 then
		_sql = _sql || '
WITH source AS (
	SELECT ' || _srcFields || '
	FROM ' || _srcDOName || '
	WHERE ' || _srcFieldsTimestampFilter || '
)
,';
	ELSE
		_sql = _sql || '
WITH source_raw AS (
	SELECT 
' || _srcFields || '
,ROW_NUMBER() OVER (PARTITION BY ' || _srcFieldsUnique || ' ' || case when length(coalesce(_srcFieldsTimestamp,''))=0 then 'ORDER BY ' || _srcFieldsUnique else 'ORDER BY ' || _srcFieldsTimestamp end || ') AS N 
	FROM ' || _srcDOName || '
	WHERE '|| _srcFieldsTimestampFilter || '
)
,source AS (
	SELECT ' || _srcFields || '
	FROM source_raw
	WHERE N=1
)
';
	end if;	
	
	_sql = '
create temp table res' || pg_backend_pid() || ' (Action text' || case when _logChanges = true then ',' || _resultFieldsDefinition else '' end  || '
)on commit drop;
' || _sql;

	_sql = _sql || '
,matchedResult as (
UPDATE ' || _dstDOName || ' as target 
SET ' || _updateFields || ' 
from source 
where ' || case when _mergeUpdate = true then _targetFieldsTimestampFilter || ' and ' || _mergeOn || ' and (' || _compareFields || ')' else 'false' end || ' 
RETURNING ''UPDATE'',' || _dstReturningFields || '
)';
	_sql = _sql || '
,notmatchedByTargetResult as (
insert into ' || _dstDOName || '(' || _dstFields || ') 
select ' || _srcInsertingFields || '
from source 
left join (select * from ' || _dstDOName || ' as target where ' || _targetFieldsTimestampFilter || ') as target on ' || _mergeOn || '
where  ' || case when _mergeInsert = true then 'target is null' else 'false' end || '
RETURNING ''INSERT'',' || _dstFields || '
)';
	_sql = _sql || '
,notmatchedBySourceResult as (
delete from ' || _dstDOName || ' as target
where ' || case when _mergeDelete = true then '(' || _targetFieldsTimestampFilter || ') and not exists (select 1 from source where ' || _mergeOn || ')' else 'false' end || '
RETURNING ''DELETE'',' || _dstReturningFields || '
)';

	_sql = _sql || '
insert into res' || pg_backend_pid() || '(action' || case when _logChanges = true then ',' || _dstFields else '' end ||  ')
select merge_action' || case when _logChanges = true then ',' || _dstFields else '' end || ' from (
select * from matchedResult
union all
select * from notmatchedByTargetResult
union all
select * from notmatchedBySourceResult
) as t(merge_action,'||_dstFields||');
';

	return _sql;

end;
$BODY$;

CREATE OR REPLACE PROCEDURE integrations.usp_runintegration(
	IN in_dataobjectid integer,
	IN in_startdatetime timestamp with time zone DEFAULT NULL::timestamp with time zone,
	IN in_enddatetime timestamp with time zone DEFAULT NULL::timestamp with time zone,
	IN in_useaction boolean DEFAULT false,
	IN in_verbose integer DEFAULT 0)
LANGUAGE 'plpgsql'
AS $BODY$
declare
	_me text = ''; 
	_procid int;
	_logCommand text = '';
	_logMessage text = '';
	_logChanges bool = false;
	_sql text = '';
	_params text = '';
	_countInsert bigint = 0;
	_countUpdate bigint = 0;
	_countDelete bigint = 0;

	_success bool = false;
	_details_in jsonb;
	_message text;
	_detail text;
	_hint text;
	_error_stack text;
	_log_id bigint; 
begin
/*
in_dataObjectId - id объекта данных из таблицы DataObject
in_StartDateTime - начало периода запроса данных (для фильтра по полям с меткой IsTimestamp из таблицы DataAttributes)
in_EndDateTime - конец периода запроса данных (для фильтра по полям с меткой IsTimestamp из таблицы DataAttributes)
in_useAction - использовать запрос из колонки action таблицы DataObject вместо генерации запроса из метаданных
in_verbose - вывод информации о работе процедуры.
	0 - ничего не выводить
	1 - вывести сгенерированный скрипт и выполнить его
	2 - вывести сгенерированный скрипт без его выполнения
*/
-- [integrations].[usp_runintegrations] @dataObjectId = 2, @useAction = 1;	

	get diagnostics _procid = pg_routine_oid;
	select nspname || '.' || proname into _me from pg_proc join pg_namespace on pronamespace = pg_namespace.oid and pg_proc.oid = _procid;
	
	_logCommand = _me||'('||in_dataobjectid||','||case when in_StartDatetime is null then 'NULL' else '''' || to_char(in_StartDatetime,'YYYY-MM-DD HH24:MI:SS') || '''' end||','||case when in_EndDateTime is null then 'NULL' else '''' || to_char(in_EndDateTime,'YYYY-MM-DD HH24:MI:SS') || '''' end||','||in_useAction||')';

	if not exists (select id from integrations.DataObject where id = in_dataObjectId) then
		RAISE 'DataObject ID error. Input ID is - %',in_dataObjectId;
	end if;
	
	select logChanges
	into _logChanges 
	from integrations.DataObject 
	where id = in_dataObjectId;

	_sql = _sql || ' do $inner'||pg_backend_pid()||'$
DECLARE 
	_startDateTime timestamp = %1$L;
	_endDateTime timestamp = %2$L;
	_mergeInsert jsonb;
	_mergeUpdate jsonb;
	_mergeDelete jsonb;
	_log_id bigint;
	_insertedDetails jsonb;
	_deletedDetails jsonb;
	_updatedDetails jsonb;
BEGIN
	if _endDateTime is null then _EndDateTime = now(); end if;
	if _startDateTime is null then _startDatetime = _EndDateTime - interval ''2 day''; end if;
';
	
	_sql = _sql || integrations.usp_generate(in_dataObjectId,in_useAction);

	if _sql is null then
		RAISE 'SQL is null. Generation error!';
	end if;	

	if _logChanges = false then	
		_sql = _sql || '
select to_jsonb(t) from (SELECT count(*) as Inserted FROM res' || pg_backend_pid() || ' WHERE Action=''INSERT'') as t(Inserted) into _mergeInsert; 
select to_jsonb(t) from (SELECT count(*) as Updated FROM res' || pg_backend_pid() || ' WHERE Action=''UPDATE'') as t(Updated) into _mergeUpdate;
select to_jsonb(t) from (SELECT count(*) as Deleted FROM res' || pg_backend_pid() || ' WHERE Action=''DELETE'') as t(Deleted) into _mergeDelete;

insert into integrations.log(dataobject_id, command,IsError) select %3$L,%4$L,false returning id into _log_id;
insert into integrations.LogDetails(log_id,Details) 
select _log_id, _mergeInsert || _mergeDelete || _mergeUpdate;
';
	else
		_sql = _sql || '
select (select ''{ "INSERT":'' || coalesce(json_agg(res' || pg_backend_pid() || '),''[]'') || ''}'' from res' || pg_backend_pid() || ' where Action =''INSERT'')::jsonb into _insertedDetails;
select (select ''{ "DELETE":'' || coalesce(json_agg(res' || pg_backend_pid() || '),''[]'') || ''}'' from res' || pg_backend_pid() || ' where Action =''DELETE'')::jsonb into _deletedDetails;
select (select ''{ "UPDATE":'' || coalesce(json_agg(res' || pg_backend_pid() || '),''[]'') || ''}'' from res' || pg_backend_pid() || ' where Action =''UPDATE'')::jsonb into _updatedDetails;

insert into integrations.log(dataobject_id, command,IsError) select %3$L,%4$L,false returning id into _log_id;
insert into integrations.LogDetails(log_id,Details)
select _log_id, _insertedDetails || _deletedDetails || _updatedDetails;
';
	end if;

	_sql = _sql || '
	drop table if exists res' || pg_backend_pid() || ';
end
$inner'||pg_backend_pid()||'$;
';

	if (in_verbose > 0) then 
		raise notice '%', '
do $outer'||pg_backend_pid()||'$ 
DECLARE 
_dataObject_id_in int = '||in_dataObjectId||';
_logCommand_in text = '''||replace(_logCommand,'''','''''')||''';
_startDateTime_in timestamp = '||case when in_StartDateTime is null then 'NULL' else '''' || to_char(in_startDatetime,'YYYY-MM-DD HH24:MI:SS') || '''' end || ';
_endDateTime_in timestamp = '||case when in_endDateTime is null then 'NULL' else '''' || to_char(in_endDateTime,'YYYY-MM-DD HH24:MI:SS') || '''' end || ';
_sql text;
BEGIN
	_sql := '''|| replace(_sql,'''','''''') || ''';
	EXECUTE format(_sql,_startDateTime_in,_endDateTime_in,_dataObject_id_in,_logCommand_in);
end 
$outer'||pg_backend_pid()||'$;
';
	end if;
	
	if (in_verbose = 2) then
		return;
	else
		execute format(_sql,in_StartDateTime, in_EndDateTime, in_dataObjectId, _logCommand);
	end if;
EXCEPTION
	when OTHERS then
	
		get stacked diagnostics
			_message = message_text,
			_detail = pg_exception_detail,
			_hint = pg_exception_hint,
			_error_stack = pg_exception_context;
			
		select json_agg(t) 
		into _details_in
		from (		
			select 
				_message
				,_detail
				,_hint
				,_error_stack
		)t(message,detail,hint,error_stack)		
		;
		insert into integrations.log(dataobject_id, command,IsError) select in_dataObjectId,_logCommand,true returning id into _log_id;
		insert into integrations.LogDetails(log_id,Details) select _log_id, _details_in;

		raise notice '%',_details_in;
end
$BODY$;