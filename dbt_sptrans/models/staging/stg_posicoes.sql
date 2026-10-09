with fonte as (

    select * from {{ source('sptrans_raw', 'raw_posicao') }}

),

convertido as (

    select
        prefixo_veiculo,
        codigo_linha,
        letreiro,
        sentido,
        acessivel,
        latitude,
        longitude,

        ta_utc,
        ta_utc at time zone 'America/Sao_Paulo' as momento_local,

        date(ta_utc at time zone 'America/Sao_Paulo')                 as data_local,
        extract(hour from ta_utc at time zone 'America/Sao_Paulo')    as hora_local,
        extract(isodow from ta_utc at time zone 'America/Sao_Paulo')  as dia_semana,

        case
            when extract(isodow from ta_utc at time zone 'America/Sao_Paulo') <= 5
                then 'util'
            when extract(isodow from ta_utc at time zone 'America/Sao_Paulo') = 6
                then 'sabado'
            else 'domingo'
        end as tipo_dia,

        ingested_at

    from fonte
    where latitude between -24.1 and -23.3
      and longitude between -47.0 and -46.3
      and ta_utc >= ingested_at - interval '15 minutes'
      and ta_utc <= ingested_at + interval '2 minutes'

)

select * from convertido