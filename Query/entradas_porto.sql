IF
	OBJECT_ID('tempdb..#CARTEIRA_PORTO') IS NOT NULL
		DROP TABLE #CARTEIRA_PORTO;

SELECT *
	INTO #CARTEIRA_PORTO
FROM OPENQUERY(LINKEDPLAN,'
	SELECT DISTINCT*
	FROM CARTEIRA
	WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''');
IF
	OBJECT_ID('tempdb..#ENTRADAS_PORTO') IS NOT NULL
		DROP TABLE #ENTRADAS_PORTO;
SELECT *
INTO #ENTRADAS_PORTO
FROM OPENQUERY(LINKEDPLAN,'
SELECT DISTINCT 
	ID_CONTR
	,ID_CARTEIRA
	,CAST(DATA_CARGA AS DATE) AS DATA_CARGA
FROM CONTRATO
	WHERE ID_CARTEIRA BETWEEN ''1150'' AND ''1154''
	AND YEAR(DATA_CARGA) = YEAR(CURRENT_DATE())
	AND MONTH(DATA_CARGA) = MONTH(CURRENT_DATE())')
SELECT 
	DATA_CARGA
	,A.ID_CARTEIRA
	,B.NOME
	,COUNT(ID_CONTR) AS QTD_DE_ENTRADAS
FROM #ENTRADAS_PORTO AS A JOIN #CARTEIRA_PORTO AS B ON A.ID_CARTEIRA = B.ID_CARTEIRA

GROUP BY 
	DATA_CARGA
	,A.ID_CARTEIRA
	,B.NOME
ORDER BY DATA_CARGA