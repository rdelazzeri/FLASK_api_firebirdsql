from urllib import response
import firebirdsql
import sqlite3
import json
from datetime import datetime
import time

def tempo(funcao):
    inicio = time.time()
    funcao
    fim = time.time()
    print(fim - inicio)


def teste():
    return('teste')

def _firebirdsql():
    con = firebirdsql.connect(
        host='localhost',
        database='/cybersul/banco/dadosadm.fdb',
        port=3050,
        user='sysdba',
        password='masterkey',
        auth_plugin_name='Srp'
    )
    cur = con.cursor()
    return cur

def _sqlite():
    conn = sqlite3.connect('c:\py\cybersul\dados_cyber.db')
    conn.row_factory = sqlite3.Row
    return conn


def _connection():
    return _firebirdsql()
    #return _sqlite()


def cyber_grupos():
    conn = _connection()
    #cur = conn.cursor()
    r = conn.execute("""
                select
                    CODIGO,
                    DESCRICAO
                from acec1201
                order by DESCRICAO
                """).fetchall()
    response = []
    for c in r:
        reccord = {
            'codigo': c[1][0:4],
            'desc': c[1][7:],
            'cod_cyber': c[0],
        }
        response.append(reccord)       
    conn.close()
    return json.dumps(response)



def cyber_unidades():
 
    UNIDS = [
            ['PC' , 'Peça'],
            ['MT' , 'Metro'],
            ['BD' , 'Balde'],
            ['FD' , 'Fardo'],
            ['GL' , 'Galão'],
            ['GR' , 'Gramas'],
            ['M2' , 'Metro Quadrado'],
            ['CX' , 'Caixa'],
            ['CT' , 'Cento'],
            ['M3' , 'Metro Cúbico'],
            ['MI' , 'Milheiro'],
            ['BB' , 'Bombona'],
            ['LT' , 'Litro'],
            ['PCT' , 'Pacote'],
            ['KW' , 'Kilowatt'],
            ['UN' , 'Unidade'],
            ['KG' , 'Quilograma'],
            ['TON' , 'Tonelada'],
            ['M', 'Metro'],
            ['NC', 'Não Cadastrado'],
    ]

    response = []
    for unid in UNIDS:
        u = {
            'unid': unid[0],
            'desc': unid[1],
        }
        print(u)
        response.append(u)
    return json.dumps(response, ensure_ascii=False).encode('utf8')


def cyber_ncm():
    conn = _connection()
    data = conn.execute("""
                select
                    NCCODIGO,
                    NCDESCRICAO
                from acec15nc
                """).fetchall()
    response = []
    for c in data:
        reccord = {
                'cod': c[0],
                'desc': c[1],
        }
        response.append(reccord)        
    conn.close()
    return json.dumps(response, ensure_ascii=False).encode('utf8')


def cyber_prod():
    conn = _connection()
    data = conn.execute("""
                select
                    CODIGO,
                    DESCRICAO,
                    COMPLEMENTO_DESC,
                    GRUPO,
                    UNIDADE_MEDIDA,
                    PESO_MERCADORIA,
                    CLASSIFICACAO_FISCAL,
                    ESTOQUE_MINIMO,
                    ESTOQUE_MAXIMO,
                    PCO_VENDA,
                    PCO_CUSTO,
                    ESTOQUE,
                    DATA_INCLUSAO,
                    DATA_MODIFICACAO
                from ACEC1101 
                """).fetchall()
    response = []
    for c in data:
        reccord = {
                'cod': c[0],
                'desc': c[1],
                'compl': c[2],
                'grupoCyber': int(c[3]) if c[3] else 0,
                'unidCyber': c[4],
                'fatorUnid': c[5],
                'pLiq': c[5],
                'NCMCyber': c[6],
                'qEstMin': c[7],
                'qEstMax': c[8],
                'prVenda':c[9],
                'prCusto': c[10],
                'qEstoque': c[11],
        }
        response.append(reccord)
    conn.close()
    return json.dumps(response, ensure_ascii=False).encode('utf8')


def cyber_composicao():
    conn = _connection()
    data = conn.execute('select prcodigo, cmcodigo, cmquantidade, ugmodificadoreg, uginseridoreg from agpc03cm').fetchall()
    response=[]
    for c in data:
        reccord = {
        'codProd': c[0],
        'codComp': c[1],
        'qtd': c[2],
        }
        response.append(reccord)
    conn.close()
    return json.dumps(response, ensure_ascii=False).encode('utf8')


def cyber_pessoa():
    response = []
    conn = _connection()
    data = conn.execute("""
            select
                OBSERVACAO,
                PESSOA_FISICAOUJURIDICA,
                NOME,
                NOME_FANTASIA,
                CGC_CNPJ,
                CLIE,
                CLENDERECO,
                CLENDNUMERO,
                CLENDCOMPLEMENTO,
                BAIRRO,
                CIDADE,
                ESTADO,
                TELEFONE1,
                TELEFONE2,
                CEP
            FROM augc0301
            ORDER BY NOME
                """).fetchall()
    for c in data:
        reccord = {}
        reccord['obs'] = c[0] if c[0] else ''
        reccord['pessoa'] = c[1] if c[1] else ''
        reccord['nome'] = c[2] if c[2] else ''
        reccord['apelido'] = c[3] if c[3] else ''
        reccord['tipo_pessoa'] = c[1] if c[4] else ''
        reccord['tipo_parc'] = 'C' if c[5] else ''
        reccord['logradouro'] = c[6] if c[6] else ''
        reccord['numero'] = c[7] if c[7] else ''
        reccord['complemento'] = c[8] if c[8] else ''
        reccord['bairro'] = c[9] if c[9] else ''
        reccord['cep'] = c[14] if c[14] else ''
        reccord['cidade'] = c[10] if c[10] else ''
        reccord['estado'] = c[11] if c[11] else ''
        reccord['fone1'] = c[12] if c[12] else ''
        reccord['fone2'] = c[13] if c[13] else ''
        response.append(reccord)
    conn.close()

## importação de fornecedores
    conn = _connection()
    data = conn.execute("""
            select
                FOBS,
                FJUR_FIS,
                FNOME,
                FNOME_FANTASIA,
                FCNPJ_CIC,
                FIE,
                FOENDERECO,
                FOENDNUMERO,
                FOENDCOMPLEMENTO,
                FBAIRRO,
                FCIDADE,
                FUF,
                FFONE1,
                FFONE2,
                FOBS2,
                FTIPO,
                FCEP
            FROM augc0501
            ORDER BY FNOME
                """).fetchall()
    i = 0
    for c in data:
        i += 1
        if c[1] == 'F':
            cpf = c[4] if c[4] else ''
            cnpj = ''
            insc_est = ''
        else:
            cnpj = c[4] if c[4] else ''
            insc_est = c[5] if c[5] else ''
            cpf = ''

        reccord = {}
        reccord['tipo_parc'] = c[15]
        reccord['pessoa'] = c[1]
        reccord['nome'] = c[2] if c[2] else ''
        reccord['apelido'] = c[3] if c[3] else ''
        reccord['cpf'] =  cpf
        reccord['cnpj'] = cnpj
        reccord['insc_est'] =  insc_est
        reccord['logradouro'] = c[6] if c[6] else ''
        reccord['numero'] = c[7] if c[7] else ''
        reccord['complemento'] = c[8] if c[8] else ''
        reccord['bairro'] = c[9]if c[9] else ''
        reccord['cep'] =  c[16] if c[16] else ''
        reccord['cidade'] = c[10] if c[10] else ''
        reccord['estado'] = c[11] if c[11] else ''
        reccord['fone1'] = c[12].replace(' ', '') if c[12] else ''
        reccord['fone2'] = c[13].replace(' ', '') if c[13] else ''
        reccord['email_contato'] = c[14] if c[14] else ''
        reccord['obs'] = c[0] if c[0] else ''

        response.append(reccord)
        print(reccord)

    print(i)        
    conn.close()
    #resposta =  json.dumps(response, ensure_ascii=False).encode('utf8')
    #print(response)
    return json.dumps(response, ensure_ascii=False).encode('utf8')
    #return('oi')



#Operacao
def cyber_plano_contas():
    conn = _connection()
    data = conn.execute("""
                select
                    CONTA,
                    DESCRICAO,
                    CONTA_DE_BANCO,
                    SENHA
                from ACXC9001
                """).fetchall()
    response = []
    for c in data:
        reccord = {}
        reccord['desc'] = c[1]
        reccord['num'] = c[0]
        reccord['cod_cyber'] = c[0]
        reccord['banco'] = c[2]
        reccord['nivel'] = c[3] if c[3] else '7'
        response.append(reccord)
    conn.close()
    return json.dumps(response, ensure_ascii=False).encode('utf8')


def trata_cnpj(cnpj):
        if cnpj:
            cnpj2 = cnpj.replace('.', '')
            cnpj2 = cnpj2.replace('-', '')
            cnpj2 = cnpj2.replace('/', '')
        else:
            cnpj2 = '04408568000105'
        return cnpj2

#nota master
def cyber_nf_entrada():
    conn = _connection()
    data = conn.execute("""
            select
                DOCUMENTO,                                     
                COD_CLI,               
                SERIE_DOC,             
                OPERACAO_DOC,              
                DATA,              
                TOTAL_DOC,             
                ICMS_DOC,              
                TRANSPORTADORA,            
                BCI,               
                OBSERVACAO_NF,             
                DESCONTO_GERAL,             
                DESPESAS,               
                FRETE,              
                TOTAL_ICMS,             
                VALOR_SEGURO,               
                TOTAL_PRODUTOS,             
                TOTAL_IPI,              
                FRETE1_2,               
                B.COD_NATUREZA,             
                B.XCCODIGO,        
                COD_OP,
                C.FCNPJ_CIC

            from acem1401
                JOIN afvc0901 B ON (OPERACAO_DOC = COD_OP)
                JOIN AUGC0501 C ON (COD_CLI = C.FCOD)
            where ENTRADA_SAIDA = 'E'
                """).fetchall()
    response = []
    for c in data:        
        reccord = {}
        reccord['num'] = c[0]
        reccord['cyber_cod_cli'] = c[1]
        reccord['cnpj'] = trata_cnpj(c[21])
        reccord['plano_contas'] = c[19]
        reccord['operacao'] = c[3] if c[3] else ''
        reccord['serie'] = c[2]
        reccord['data_emissao'] = c[4]
        reccord['desconto'] = c[10]
        reccord['valor_frete'] = c[12]
        reccord['valor_seguro'] = c[14]
        reccord['tipo_frete'] = c[17] if c[17] else ''
        
        data_itens = conn.execute(f"""
                    select
                        CODIGOPRODUTO,
                        QUNATIDADEPRODUTO,
                        VALOR_UNITARIOPRODUTO,
                        ICMS_ITEM,
                        IPI_ITEM,
                        OBSERVACAO_PRODUTO
                    FROM ACEM14IT
                    where DOCUMENTO = '{c[0]}'
                        AND COD_CLI = '{c[1]}'
                    """)
        itens = []
        for i in data_itens:
            item = {}
            item['cyber_codprod'] = i[0]
            item['qtd'] = i[1]
            item['val_unit'] = round(i[2],4)
            item['icms'] = c[3]
            item['ipi'] = c[4]
            item['obs'] = c[5]
            itens.append(item)
        reccord['itens'] = itens
        response.append(reccord)
    conn.close()
    #return json.dumps(response, ensure_ascii=False).encode('utf8')
    return json.dumps(response, indent=4, sort_keys=True, default=str)







####---------------------------------entregas

#Operacao
def cyber_sinc_operacao_saida(request):
    template_name = 'cyber_sinc/cyber_sinc.html'

    #usar se precisar deletar
    Operacao.objects.all().delete()

    conn = firebirdsql.connect(dsn='localhost:/cybersul/banco/dadosadm.fdb', user='sysdba', password='masterkey', charset='ISO8859_1')
    cur = conn.cursor()
    cur.execute("""
                    select cod_natureza, xccodigo
                    from afvc0901
                    where tipo_op in ('2', '6', '9', 'c', 'd')
                    group by cod_natureza, xccodigo
                """)
    
    #NF_entrada.objects.all().delete()
    #Operacao.objects.all().delete()
    #Plano_contas.objects.exclude(num = '1').delete()
    
    for c in cur.fetchall():
        try:
            pl = Plano_contas.objects.get(num = c[1])
        except:
            pl = None

        op = Operacao()
        op.desc = c[0]
        op.natureza_operacao = c[0]
        op.tipo = '1'
        op.CFOP = c[0]
        op.origem_mercadoria = '0'
        op.conta_caixa = pl
        op.save()
        
    conn.close()
    
    context = {'context': 'plano de contas feito'}
    return render(request, template_name, context)

#nota master
def cyber_sinc_nf_s(request):
    template_name = 'cyber_sinc/cyber_sinc.html'

    Entrega.objects.all().delete()

    conn = firebirdsql.connect(dsn='localhost:/cybersul/banco/dadosadm.fdb', user='sysdba', password='masterkey', charset='ISO8859_1')
    cur = conn.cursor()
    cur.execute("""
            select
                    DOCUMENTO,                                     
                    COD_CLI,               
                    SERIE_DOC,             
                    OPERACAO_DOC,              
                    DATA,              
                    TOTAL_DOC,             
                    ICMS_DOC,              
                    A.TRANSPORTADORA,            
                    BCI,               
                    OBSERVACAO_NF,             
                    DESCONTO_GERAL,             
                    DESPESAS,               
                    FRETE,              
                    TOTAL_ICMS,             
                    VALOR_SEGURO,               
                    TOTAL_PRODUTOS,             
                    TOTAL_IPI,              
                    FRETE1_2,               
                    B.COD_NATUREZA,             
                    B.XCCODIGO,        
                    COD_OP,
                    C.CGC_CNPJ

                from acem1401 A
                JOIN afvc0901 B ON (OPERACAO_DOC = COD_OP)
                JOIN AUGC0301 C ON (COD_CLI = C.CODIGO_CLIENTE)
                where data > '2022-01-01'
                        AND ENTRADA_SAIDA = 'S'
                order by documento
                """)

    #for c in cur.fetchall():
    
    #cadastrar pedido generico
    try:
        ped = Pedido.objects.get(num = 0)
    except:
        ped = Pedido()
        ped.num = 0
        ped.obs = 'Pedido Genérico'
        ped.save()
        prod = Prod.objects.get(cod = '999')
        pedit = Pedido_item()
        pedit.pedido = ped
        pedit.produto = prod
        pedit.save()
        print('pedido generico salvo')

    c =  cur.fetchone()
    while c:
        print('NF: %s - CNPJ: %s',(c[0], c[21]))
        doc = c[21]
        if len(doc) > 14:
            cnpj1 = str(c[21])
            cnpj2 = cnpj1.replace('.', '')
            cnpj2 = cnpj2.replace('-', '')
            cnpj2 = cnpj2.replace('/', '')
            #print(cnpj2)
            #parc = Parceiro.objects.filter(cnpj = cnpj_limpo)
            parc = Parceiro.objects.filter(cnpj = cnpj2) if Parceiro.objects.filter(cnpj = cnpj2) else Parceiro.objects.filter(cnpj = '04408568000105')
        else:
            cpf = doc.replace('.', '')
            cpf = doc.replace('-', '')
            #print(cnpj2)
            #parc = Parceiro.objects.filter(cnpj = cnpj_limpo)
            parc = Parceiro.objects.filter(cpf = cpf) if Parceiro.objects.filter(cpf = cpf) else Parceiro.objects.filter(cnpj = '04408568000105')
        try:
            #ct = Plano_contas.objects.get(num = c[19])
            op = Operacao.objects.get(CFOP = c[18]) 
        except:
             op = Operacao.objects.get(CFOP = '05901')

        try:
            nf = Entrega.objects.get(num_nf = c[0])
        except:
            nf = Entrega()


        dt = str(c[4])[0:10]

        nf.num = c[0]
        nf.num_nf = c[0]
        nf.cliente = parc[0]
        nf.operacao = op
        nf.status = 2
        nf.pedido_origem = ped
        nf.data_cadastro = datetime.strptime(dt, '%Y-%m-%d')
        nf.data_emissao = datetime.strptime(dt, '%Y-%m-%d')
        nf.valor_frete = c[12]
        nf.tipo_frete = c[17] if c[17] else '2'
        nf.save()

        c = cur.fetchone()
        
    conn.close()
    print('nf entrada ok')
    context = {'context': 'Notas de saida feito'}
    return render(request, template_name, context)


def cyber_sinc_nf_si(request):
    
    template_name = 'cyber_sinc/cyber_sinc.html'
    conn = firebirdsql.connect(dsn='localhost:/cybersul/banco/dadosadm.fdb', user='sysdba', password='masterkey', charset='ISO8859_1')
    cur = conn.cursor()

    Entrega_item.objects.all().delete()

    ## importação de itens das 00notas
    cur.execute("""
            select
                DOCUMENTO,
                COD_CLI,
                CODIGOPRODUTO,
                QUNATIDADEPRODUTO,
                VALOR_UNITARIOPRODUTO,
                ICMS_ITEM,
                IPI_ITEM,
                OBSERVACAO_PRODUTO
            FROM ACEM14IT
            where data > '2022-01-01'
                    AND SERIE = 'E1'
            
                """)
    #Prod.objects.all().delete()
    #Parceiro.objects.all().delete()

    #for c in cur.fetchall():

    pedit = Pedido_item.objects.filter(pedido__num = 0).first()
    print('pedit: ', pedit)
    c =  cur.fetchone()
    while c:

        try:
            prod = Prod.objects.get(cod = c[2])
        except:
            prod = Prod.objects.get(cod = '999')

        try:
            nf = Entrega.objects.get(num_nf = int(c[0]) )
            print('nf recuperada', c[0])
            nfi = Entrega_item()
            nfi.entrega = nf
            nfi.produto = prod
            nfi.pedido_item = pedit
            nfi.qtd = c[3]
            nfi.pr_unit = c[4]
            nfi.aliq_ICMS = c[5]
            nfi.aliq_IPI = c[6]
            nfi.obs = c[7]
            nfi.save()
            print(nfi.produto.desc)
        except:
            print('nf nao localizada: %s',(c[0]) )
        
        c = cur.fetchone()

    conn.close()

    context = {'context': 'Itens da nfe entrada feito'}
    return render(request, template_name, context)
