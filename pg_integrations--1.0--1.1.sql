\echo Use "CREATE EXTENSION ..." to load this file! \quit

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
			case when data_type in (''text'',''bytea'',''bit'',''bit varying'',''character'',''character varying'',''json'',''jsonb'',''xml'') and character_maximum_length is null then -1 else character_maximum_length end as max_length
		from information_schema.columns where table_name = ''temptest',pg_backend_pid(),'''
	;
	');
	execute(_sql);
	
	_sql = concat('
	select 
		string_agg(SourceName,E'',''),
		string_agg(coalesce(DestinationName,SourceName),E'',''),
		string_agg(''source.'' || SourceName, E'',''),
		string_agg(case when max_length = -1 then  ''case when pg_column_size(target.'' || coalesce(DestinationName,SourceName) || '') > 1024 then sha512(target.'' || coalesce(DestinationName,SourceName) || ''::text::bytea)::text else target.'' || coalesce(DestinationName,SourceName) || ''::text end'' else ''target.'' || coalesce(DestinationName,SourceName) end, E'',''),
		string_agg(concat(coalesce(DestinationName,SourceName),''=source.'',SourceName),E'',''),
		string_agg(concat(concat(coalesce(DestinationName,SourceName)),'' '', case when max_length = -1 then ''text'' else system_type_name end),E'','')
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
insert into ' || _dstDOName || ' as target(' || _dstFields || ') 
select ' || _srcInsertingFields || '
from source 
left join (select * from ' || _dstDOName || ' as target where ' || _targetFieldsTimestampFilter || ') as target on ' || _mergeOn || '
where  ' || case when _mergeInsert = true then 'target is null' else 'false' end || '
RETURNING ''INSERT'',' || _dstReturningFields || '
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