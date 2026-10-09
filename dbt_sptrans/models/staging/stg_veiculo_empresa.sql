select
    prefixo_veiculo,
    codigo_empresa,
    primeiro_visto,
    ultimo_visto,
    vezes_visto
from {{ source('sptrans_raw', 'raw_veiculo_empresa') }}