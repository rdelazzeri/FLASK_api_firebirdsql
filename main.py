
from flask import Flask
import firebirdsql
import time
from .services import *


app = Flask(__name__)

@app.route("/")
def hello_world():
    response = teste()
    return response


@app.route("/firebird")
def connection_firebird():
    #conn = firebirdsql.connect(dsn=r'rafael-pc:\cybersul\banco\DADOSADM.FDB', user='SYSDBA', password='masterkey', charset='ISO8859_1')
    con = firebirdsql.connect(
        host='localhost', port=3050, database='/py/restore5.fdb',
        user='sysdba', password='masterkey', auth_plugin_name='Srp'
    )
    print('connection open')
    con.close()
    return('conexao ok')


@app.route("/fire")
def fire():
    conn = firebirdsql.connect(
        host='localhost',
        database='/cybersul/banco/dadosadm.fdb',
        port=3050,
        user='sysdba',
        password='masterkey',
        auth_plugin_name='Srp'
    )
    cur = conn.cursor()
    cur.execute("select descricao from acec1101")
    for c in cur.fetchall():
        print(c[0])
    conn.close()
    return('oi')

@app.route("/cyber")
def cyber_sinc_operacao():
    con = firebirdsql.connect(dsn='127.0.0.1:/cybersul/banco/dadosadm.fdb', user='sysdba', password='masterkey')

    con.close()
    
    context = {'context': 'plano de contas feito'}
    return 'oi'


@app.route("/grupos")
def grupos():
    return cyber_grupos()


@app.route("/unidades")
def unidades():
    return cyber_unidades()


@app.route("/ncm")
def ncm():
    return cyber_ncm()


@app.route("/produtos")

def produtos():
    response = {}
    inicio = time.time()
    dados = cyber_prod()
    response['data']: dados
    fim = time.time()
    response['tempo'] = fim - inicio
    return json.dumps(response)


@app.route("/composicao")
def composicao():
    return cyber_composicao()


@app.route("/pessoa")
def pessoa():
    return cyber_pessoa()

@app.route("/plano_contas")
def plano_contas():
    return cyber_plano_contas()

@app.route("/nf_entrada")
def nf_entrada():
    return cyber_nf_entrada()