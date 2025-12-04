from db.connection import init_connection
import pandas as pd
import streamlit as st

def execute_query_fn(queryFn):
    conn = init_connection()
    if conn is not None:
        try:
            # Acessar a coleção
            db = conn['bd_unesp_marketplace']  # Substitua <dbname> pelo nome do seu banco de dados

            # Executar a consulta
            cursor = queryFn(db)

            # Converter o cursor para uma lista de documentos e depois para um DataFrame
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as e:
            st.error(f"Erro ao executar a consulta: {e}")
        finally:
            conn.close()  # Certifique-se de fechar a conexão
    else:
        st.error("Conexão ao banco de dados não pôde ser estabelecida.")

def get_ticket_medio_por_forma_pagamento_query_fn(db):
    return db.pedido.aggregate([
        # 1. AGRUPAMENTO (GROUP BY formaPagto)
        {
            "$group": {
            "_id": "$formapagto", # Agrupa pela forma de pagamento
            "ticket_medio": { "$avg": "$valortotal" } # Calcula a média do valor total
            }
        },
        # 2. PROJEÇÃO (SELECT)
        {
            "$project": {
            "_id": 0,
            "formaPagto": "$_id",
            # Simula o arredondamento para duas casas decimais
            "ticket_medio": { "$round": ["$ticket_medio", 2] }
            }
        }
    ])

def get_top_selling_products_query_fn(db):
    return db.pedido.aggregate([
        # 1. FILTRO (WHERE pd.statuspagto = 'Pago')
        {
            "$match": {
            "statuspagto": "Pago"
            }
        },
        # 2. Desestrutura o array 'compras' para obter cada item comprado
        {
            "$unwind": "$compras"
        },
        # 3. JOIN: Compras com Item (c.iditem = i.iditem) para obter o idproduto
        {
            "$lookup": {
            "from": "item",
            "localField": "compras.iditem",
            "foreignField": "iditem",
            "as": "detalhes_item"
            }
        },
        {
            "$unwind": "$detalhes_item"
        },
        # 4. JOIN: Item com Produto (i.idproduto = p.idproduto) para obter nome, carta e idcolecao
        {
            "$lookup": {
            "from": "produto",
            "localField": "detalhes_item.idproduto",
            "foreignField": "idproduto",
            "as": "detalhes_produto"
            }
        },
        {
            "$unwind": "$detalhes_produto"
        },
        # 5. JOIN: Produto com Colecao (p.idcolecao = cl.idcolecao) para totalcartas
        {
            "$lookup": {
            "from": "colecao",
            "localField": "detalhes_produto.idcolecao",
            "foreignField": "idcolecao",
            "as": "detalhes_colecao"
            }
        },
        {
            "$unwind": {
            "path": "$detalhes_colecao",
            "preserveNullAndEmptyArrays": True # LEFT JOIN
            }
        },
        # 6. AGRUPAMENTO (GROUP BY p.nome, cr.numeracao, cl.totalcartas) e SUM(c.quantidade)
        {
            "$group": {
            "_id": {
                "idProduto": "$detalhes_produto.idproduto",
                "nome": "$detalhes_produto.nome",
                "numeracao": "$detalhes_produto.carta.numeracao", # Campo 'carta' incorporado em 'produto'
                "totalCartas": "$detalhes_colecao.totalcartas"
            },
            "total_vendas": { "$sum": "$compras.quantidade" }
            }
        },
        # 7. ORDENAÇÃO (ORDER BY total_vendas DESC)
        {
            "$sort": {
            "total_vendas": -1
            }
        },
        # 8. LIMITE (LIMIT 10)
        {
            "$limit": 10
        },
        # 9. PROJEÇÃO (SELECT e Formatação da Numeração)
        {
            "$project": {
            "_id": 0,
            "nome": "$_id.nome",
            "total_vendas": 1,
            # Implementação da lógica CASE/CONCAT para a numeração da carta
            "numeracao_carta": {
                "$cond": {
                    "if": "$_id.numeracao", # Se existir numeração
                    "then": {
                        "$concat": [
                        { "$toString": "$_id.numeracao" },
                        " / ",
                        { "$toString": { "$ifNull": ["$_id.totalCartas", 0] } }
                        ]
                    },
                    "else": ""
                }
            }
            }
        }
    ])

def get_top_artists_query_fn(db):
    return db.produto.aggregate([
        # 1. FILTRO: Garante que estamos lidando apenas com cartas (produto.tipoproduto = 'C')
        {
            "$match": {
            "tipoproduto": "C"
            }
        },
        # 2. AGRUPAMENTO (GROUP BY c.artista)
        {
            "$group": {
            "_id": "$carta.artista",
            "total_cartas": { "$sum": 1 }
            }
        },
        # 3. ORDENAÇÃO (ORDER BY total_cartas DESC)
        {
            "$sort": {
            "total_cartas": -1
            }
        },
        # 4. LIMITE (LIMIT 3)
        {
            "$limit": 3
        },
        # 5. PROJEÇÃO (SELECT)
        {
            "$project": {
            "_id": 0,
            "artista": "$_id",
            "total_cartas": 1
            }
        }
    ])

def get_top_collections_by_unique_cards_query_fn(db):
    return db.produto.aggregate([
        # 1. FILTRO: Apenas produtos que são cartas (tipoproduto = 'C')
        {
            "$match": {
            "tipoproduto": "C"
            }
        },
        # 2. JOIN (Lookup): Produto (p) com Colecao (co)
        {
            "$lookup": {
            "from": "colecao",
            "localField": "idcolecao",
            "foreignField": "idcolecao",
            "as": "detalhes_colecao"
            }
        },
        {
            "$unwind": "$detalhes_colecao"
        },
        # 3. AGRUPAMENTO (GROUP BY co.nome) - Contando ID de produto distinto
        {
            "$group": {
            # Agrupamos pelo ID e nome da coleção
            "_id": {
                "idcolecao": "$idcolecao",
                "nome": "$detalhes_colecao.nome"
            },
            # Contamos o total de documentos, que aqui representam cartas únicas
            "total_cartas_unicas": { "$sum": 1 }
            }
        },
        # 4. ORDENAÇÃO (ORDER BY total_cartas_unicas DESC)
        {
            "$sort": {
            "total_cartas_unicas": -1
            }
        },
        # 5. LIMITE (LIMIT 3)
        {
            "$limit": 3
        },
        # 6. PROJEÇÃO (SELECT)
        {
            "$project": {
            "_id": 0,
            "nome": "$_id.nome",
            "total_cartas_unicas": 1
            }
        }
    ])
            
def get_price_volatility_query_fn(db):
    return db.item.aggregate([
        # 1. JOIN (Lookup): Item (i) com Produto (p) usando idproduto
        {
            "$lookup": {
            "from": "produto",
            "localField": "idproduto",
            "foreignField": "idproduto",
            "as": "detalhes_produto"
            }
        },
        {
            "$unwind": "$detalhes_produto"
        },
        # 2. FILTRO: Apenas cartas (tipoproduto = 'C')
        {
            "$match": {
            "detalhes_produto.tipoproduto": "C"
            }
        },
        # 3. AGRUPAMENTO (GROUP BY p.nome, c.numeracao)
        {
            "$group": {
            "_id": {
                "nome": "$detalhes_produto.nome",
                "numeracao": "$detalhes_produto.carta.numeracao"
            },
            "precoMaximo": { "$max": "$precounitario" },
            "precoMinimo": { "$min": "$precounitario" },
            # Calcula o Desvio Padrão
            "desvio_padrao": { "$stdDevSamp": "$precounitario" }
            }
        },
        # 4. CÁLCULO: Calcula a diferença de preço (MAX - MIN)
        {
            "$addFields": {
            "diferenca_preco": { "$subtract": ["$precoMaximo", "$precoMinimo"] }
            }
        },
        # 5. ORDENAÇÃO (ORDER BY diferenca_preco DESC, desvio_padrao DESC)
        {
            "$sort": {
            "diferenca_preco": -1,
            "desvio_padrao": -1
            }
        },
        # 6. LIMITE (LIMIT 10)
        {
            "$limit": 10
        },
        # 7. PROJEÇÃO (SELECT e arredondamentos)
        {
            "$project": {
            "_id": 0,
            "produto_nome": "$_id.nome",
            "carta_numero": "$_id.numeracao",
            "diferenca_preco": { "$round": ["$diferenca_preco", 2] },
            "desvio_padrao": { "$round": ["$desvio_padrao", 2] }
            }
        }
    ])
