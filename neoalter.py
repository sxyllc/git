 # -*- coding: utf-8 -*-



import subprocess
import time


def alter(table):
    conf_data = ''
    table_str = "dbms.default_database="
    with open(r'D:\neo4j\neo4j-community-4.0.7\conf\neo4j.conf', "r", encoding="utf-8") as f:
        for line in f.readlines():
            if table_str in line:
                line = 'dbms.default_database=%s\n' %(table)
            conf_data += line
    with open(r'D:\neo4j\neo4j-community-4.0.7\conf\neo4j.conf',"w",encoding="utf-8") as f:
        f.write(conf_data)
    p = subprocess.Popen("neo4j.bat console")
    time.sleep(7)
    p.kill()


if __name__ == "__main__":
    alter('neo4j')

