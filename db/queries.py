from db.connection import init_connection
import pandas as pd
import streamlit as st

def execute_query(collection_name, filter_query=None):
    conn = init_connection()
    if conn is not None:
        try:
            # Acessar a coleção
            db = conn['bd_unesp_marketplace']  # Substitua <dbname> pelo nome do seu banco de dados
            collection = db[collection_name]

            # Executar a consulta
            if filter_query is None:
                cursor = collection.find()  # Buscar todos os documentos
            else:
                cursor = collection.find(filter_query)  # Aplicar o filtro

            # Converter o cursor para uma lista de documentos e depois para um DataFrame
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as e:
            st.error(f"Erro ao executar a consulta: {e}")
        finally:
            conn.close()  # Certifique-se de fechar a conexão
    else:
        st.error("Conexão ao banco de dados não pôde ser estabelecida.")

def get_ticket_medio_por_forma_pagamento_query():
    return "SELECT formaPagto, AVG(valorTotal) AS ticket_medio \
        FROM Pedido \
        GROUP BY formaPagto;"

def get_top_selling_products_query():
    return "SELECT p.nome, SUM(c.quantidade) AS total_vendas, \
        CASE  \
            WHEN cr.numeracao IS NOT NULL THEN CONCAT(CAST(cr.numeracao AS TEXT), ' / ', COALESCE(cl.totalcartas, 0)) \
            ELSE '' \
        END AS numeracao_carta \
        FROM produto p \
        JOIN item i ON p.idproduto = i.idproduto \
        JOIN compra c ON i.iditem = c.iditem \
        JOIN pedido pd ON c.idpedido = pd.idpedido \
        LEFT JOIN carta cr ON p.idproduto = cr.idproduto \
        LEFT JOIN colecao cl on p.idcolecao = cl.idcolecao \
        WHERE pd.statuspagto = 'Pago' \
        GROUP BY p.nome, cr.numeracao, cl.totalcartas \
        ORDER BY total_vendas DESC \
        LIMIT 10;"

def get_top_artists_query():
    return "SELECT c.artista, COUNT(*) AS total_cartas \
        FROM carta c \
        GROUP BY c.artista \
        ORDER BY total_cartas DESC \
        LIMIT 3;"

def get_top_collections_by_unique_cards_query():
    return "SELECT co.nome, COUNT(DISTINCT c.idproduto) AS total_cartas_unicas \
        FROM colecao co \
        JOIN produto p ON co.idcolecao = p.idcolecao \
        JOIN carta c ON p.idproduto = c.idproduto \
        GROUP BY co.nome \
        ORDER BY total_cartas_unicas DESC \
        LIMIT 3;"
            
def get_price_volatility_query():
    return "SELECT \
        p.nome AS produto_nome, \
        c.numeracao AS carta_numero,\
        MAX(i.precoUnitario) - MIN(i.precoUnitario) AS diferenca_preco,\
        STDDEV(i.precoUnitario) AS desvio_padrao\
    FROM carta c\
    JOIN produto p ON c.idproduto = p.idproduto\
    JOIN item i ON p.idproduto = i.idproduto\
    GROUP BY p.nome, c.numeracao\
    ORDER BY diferenca_preco DESC, desvio_padrao DESC\
    LIMIT 10;"
