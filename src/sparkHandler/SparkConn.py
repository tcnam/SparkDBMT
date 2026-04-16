from pathlib import Path
from typing import List, Tuple, Union, Dict, Any
from logging import Logger
from jaydebeapi import(
    Connection, Cursor
    ,connect as jdbcConnect
)
import sqlparse

from src.sparkHandler.SparkJob import SparkJob

class SparkConn:
    def __init__(self, **kwargs):
        self.envVars: Dict[str, str] = kwargs.get("envVars", {})
        self.paramVars: Dict[str, str] = kwargs.get("paramVars", {})
        self.conn: Connection = None
        self.cursor: Cursor = None
    
    def createConn(self) -> None:
        try:
            self.conn: Connection = jdbcConnect(
                jclassname=self.envVars['driverClass']
                ,url=self.envVars['jdbcUrl']
                ,driver_args=[self.envVars['username'], self.envVars['password']]
                ,jars=self.envVars['jarPath']
            )
            self.cursor: Cursor = self.conn.cursor()
        except Exception as e:
            # logger.info(f"Error create connection with given config info: {e}")
            raise
    
    def closeConn(self) -> None:
        self.cursor.close()
        self.conn.close()
    
    def testConn(self) -> None:
        if self.conn and self.cursor:
            try:
                self.cursor.execute(self.envVars.get("testQuery", ""))
                print(self.cursor.fetchall())
                # logger.info(self.cursor.fetchall())

            except Exception as e:
                print(f"Error when testing connection: {e}")
    
    def execute(self, queryFilePath: Path) -> Tuple[bool]:
        if self.conn is None:
            raise ConnectionError(
                "No database connection session. Call createConn() first"
            )
        try:
            with open(file=queryFilePath, mode='r') as io:
                sqlContent: str = io.read()
            statements: List[str] = sqlparse.split(sqlContent)
            for ind, statement in enumerate(statements):
                print(f"Ind: {ind}")
                print(statement)
                sparkJob: SparkJob = SparkJob(
                    paramVars = self.paramVars
                    ,statement = statement
                )
                finalStatement: str = sparkJob.prepareStatement()
                if finalStatement.strip():
                    self.cursor.execute(finalStatement)

        except Exception as e:
            # self.conn.rollback()
            print(f"Error executing query: {e}")