{#
  `is_current` é redundante com `valid_to` de propósito (é o filtro barato
  que todo consumo da dimensão usa), então os dois têm que concordar:
  versão vigente = valid_to nulo, versão fechada = valid_to preenchido.

  Se divergirem, consultas que filtram por `is_current` e consultas que
  filtram por `valid_to is null` passam a devolver conjuntos diferentes da
  mesma dimensão — o tipo de inconsistência que só aparece muito depois,
  num número que não bate.

  Passa quando não retorna nenhuma linha.
#}

select
    portfolio_version_key,
    customer_id,
    valid_from,
    valid_to,
    is_current
from {{ ref('dim_client_portfolio') }}
where (is_current and valid_to is not null)
   or (not is_current and valid_to is null)
