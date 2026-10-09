--
-- PostgreSQL database dump
--

\restrict lvigMXniObpXdQyCg0yFNfEuLIL33yuoWmP9JVL1uRhaIuKg8vmk1HUnfc3quAd

-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: sptrans_raw; Type: SCHEMA; Schema: -; Owner: airflow
--

CREATE SCHEMA sptrans_raw;


ALTER SCHEMA sptrans_raw OWNER TO airflow;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: etl_execucao; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.etl_execucao (
    id bigint NOT NULL,
    dag_id text NOT NULL,
    run_id text NOT NULL,
    executado_em timestamp with time zone DEFAULT now() NOT NULL,
    status text NOT NULL,
    recebidos integer,
    deduplicados integer,
    gravados integer,
    mensagem text
);


ALTER TABLE sptrans_raw.etl_execucao OWNER TO airflow;

--
-- Name: etl_execucao_id_seq; Type: SEQUENCE; Schema: sptrans_raw; Owner: airflow
--

CREATE SEQUENCE sptrans_raw.etl_execucao_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sptrans_raw.etl_execucao_id_seq OWNER TO airflow;

--
-- Name: etl_execucao_id_seq; Type: SEQUENCE OWNED BY; Schema: sptrans_raw; Owner: airflow
--

ALTER SEQUENCE sptrans_raw.etl_execucao_id_seq OWNED BY sptrans_raw.etl_execucao.id;


--
-- Name: raw_corredores; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_corredores (
    codigo_corredor integer NOT NULL,
    nome_corredor text,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE sptrans_raw.raw_corredores OWNER TO airflow;

--
-- Name: raw_empresas; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_empresas (
    codigo_empresa integer NOT NULL,
    nome_empresa text,
    codigo_area smallint,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE sptrans_raw.raw_empresas OWNER TO airflow;

--
-- Name: raw_linhas; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_linhas (
    codigo_linha integer NOT NULL,
    letreiro text,
    letreiro_sufixo text,
    circular boolean,
    sentido smallint,
    terminal_principal text,
    terminal_secundario text,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE sptrans_raw.raw_linhas OWNER TO airflow;

--
-- Name: raw_paradas; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_paradas (
    codigo_parada text NOT NULL,
    codigo_linha integer NOT NULL,
    nome_parada text,
    endereco text,
    latitude numeric(10,7),
    longitude numeric(10,7),
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE sptrans_raw.raw_paradas OWNER TO airflow;

--
-- Name: raw_posicao; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_posicao (
    prefixo_veiculo text NOT NULL,
    ta_utc timestamp with time zone NOT NULL,
    codigo_linha integer NOT NULL,
    letreiro text,
    sentido smallint,
    acessivel boolean,
    latitude numeric(10,7) NOT NULL,
    longitude numeric(10,7) NOT NULL,
    hora_consulta text,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE sptrans_raw.raw_posicao OWNER TO airflow;

--
-- Name: raw_veiculo_empresa; Type: TABLE; Schema: sptrans_raw; Owner: airflow
--

CREATE TABLE sptrans_raw.raw_veiculo_empresa (
    prefixo_veiculo text NOT NULL,
    codigo_empresa integer NOT NULL,
    primeiro_visto timestamp with time zone DEFAULT now() NOT NULL,
    ultimo_visto timestamp with time zone DEFAULT now() NOT NULL,
    vezes_visto integer DEFAULT 1 NOT NULL
);


ALTER TABLE sptrans_raw.raw_veiculo_empresa OWNER TO airflow;

--
-- Name: etl_execucao id; Type: DEFAULT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.etl_execucao ALTER COLUMN id SET DEFAULT nextval('sptrans_raw.etl_execucao_id_seq'::regclass);


--
-- Name: etl_execucao etl_execucao_pkey; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.etl_execucao
    ADD CONSTRAINT etl_execucao_pkey PRIMARY KEY (id);


--
-- Name: raw_corredores pk_raw_corredores; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_corredores
    ADD CONSTRAINT pk_raw_corredores PRIMARY KEY (codigo_corredor);


--
-- Name: raw_empresas pk_raw_empresas; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_empresas
    ADD CONSTRAINT pk_raw_empresas PRIMARY KEY (codigo_empresa);


--
-- Name: raw_linhas pk_raw_linhas; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_linhas
    ADD CONSTRAINT pk_raw_linhas PRIMARY KEY (codigo_linha);


--
-- Name: raw_paradas pk_raw_paradas; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_paradas
    ADD CONSTRAINT pk_raw_paradas PRIMARY KEY (codigo_parada, codigo_linha);


--
-- Name: raw_posicao pk_raw_posicao; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_posicao
    ADD CONSTRAINT pk_raw_posicao PRIMARY KEY (prefixo_veiculo, ta_utc);


--
-- Name: raw_veiculo_empresa pk_raw_veiculo_empresa; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.raw_veiculo_empresa
    ADD CONSTRAINT pk_raw_veiculo_empresa PRIMARY KEY (prefixo_veiculo);


--
-- Name: etl_execucao uq_etl_execucao; Type: CONSTRAINT; Schema: sptrans_raw; Owner: airflow
--

ALTER TABLE ONLY sptrans_raw.etl_execucao
    ADD CONSTRAINT uq_etl_execucao UNIQUE (dag_id, run_id);


--
-- PostgreSQL database dump complete
--

\unrestrict lvigMXniObpXdQyCg0yFNfEuLIL33yuoWmP9JVL1uRhaIuKg8vmk1HUnfc3quAd

