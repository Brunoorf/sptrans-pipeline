with posicoes as (

    select * from {{ ref('stg_posicoes') }}

),

agregado as (

    select
        data_local,
        hora_local,
        tipo_dia,
        count(distinct prefixo_veiculo)                         as veiculos,
        count(distinct codigo_linha)                            as linhas,
        count(*)                                                as observacoes,
        count(distinct prefixo_veiculo)
            filter (where acessivel)                            as veiculos_acessiveis,
        round(
            100.0 * count(distinct prefixo_veiculo) filter (where acessivel)
                  / nullif(count(distinct prefixo_veiculo), 0)
        , 1)                                                    as pct_acessivel

    from posicoes
    group by data_local, hora_local, tipo_dia

)

select * from agregado