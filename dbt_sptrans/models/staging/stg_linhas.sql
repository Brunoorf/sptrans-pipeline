with fonte as (

    select * from {{ source('sptrans_raw', 'raw_linhas') }}

),

renomeado as (

    select
        codigo_linha,
        letreiro,
        sentido,

        case
            when sentido = 1 then 'ida'
            when sentido = 2 then 'volta'
        end as sentido_descricao,

        case
            when sentido = 1 then terminal_principal
            else terminal_secundario
        end as terminal_origem,

        case
            when sentido = 1 then terminal_secundario
            else terminal_principal
        end as terminal_destino,

        btrim(terminal_principal)  as terminal_principal,
        btrim(terminal_secundario) as terminal_secundario,

        ingested_at,
        updated_at

    from fonte

)

select * from renomeado