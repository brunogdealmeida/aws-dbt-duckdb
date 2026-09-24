{#
  Garantia estrutural de uma SCD Tipo 2: as versões de uma mesma chave
  natural não podem se sobrepor no tempo.

  Pega os dois defeitos que corrompem o join point-in-time:
    - sobreposição (valid_to > valid_from da versão seguinte): um fato na
      janela sobreposta casaria com duas versões e seria contado em dobro
      no gold;
    - versão não fechada (valid_to null) que já tem uma sucessora: ficou
      aberta quando devia ter sido fechada, o que também duplica o fato.

  Buracos (valid_to < valid_from da seguinte) NÃO são defeito e por isso
  não entram aqui: acontecem quando a chave sai do snapshot e volta depois
  — a estratégia fecha a versão na saída e abre uma nova no retorno, e o
  intervalo entre as duas é justamente o período em que aquele cliente não
  tinha carteira nenhuma. Tratar isso como erro daria falso positivo (foi
  o que aconteceu ao validar o cenário de saída-e-retorno).

  Passa quando não retorna nenhuma linha.
#}

with versions as (

    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id order by valid_from
        ) as next_valid_from
    from {{ ref('dim_client_portfolio') }}

)

select
    customer_id,
    valid_from,
    valid_to,
    next_valid_from
from versions
where next_valid_from is not null
  and (valid_to is null or valid_to > next_valid_from)
