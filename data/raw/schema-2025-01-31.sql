--
-- PostgreSQL database dump
--

-- Dumped from database version 14.12
-- Dumped by pg_dump version 17.2 (Debian 17.2-1.pgdg120+1)

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
-- Name: courtlistener; Type: DATABASE; Schema: -; Owner: django
--
DROP DATABASE IF EXISTS courtlistener;

CREATE DATABASE courtlistener WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'en_US.UTF-8';


ALTER DATABASE courtlistener OWNER TO django;

\connect courtlistener

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: audio_audio; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.audio_audio (
    id integer NOT NULL,
    source character varying(10) NOT NULL,
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    judges text,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    sha1 character varying(40) NOT NULL,
    download_url character varying(500),
    local_path_mp3 character varying(100) NOT NULL,
    local_path_original_file character varying(100) NOT NULL,
    duration smallint,
    processing_complete boolean NOT NULL,
    date_blocked date,
    blocked boolean NOT NULL,
    docket_id integer,
    stt_status smallint NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    stt_source smallint,
    stt_transcript text NOT NULL
);


ALTER TABLE public.audio_audio OWNER TO django;

--
-- Name: audio_audio_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.audio_audio_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.audio_audio_id_seq OWNER TO django;

--
-- Name: audio_audio_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.audio_audio_id_seq OWNED BY public.audio_audio.id;


--
-- Name: audio_audio_panel; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.audio_audio_panel (
    id integer NOT NULL,
    audio_id integer NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.audio_audio_panel OWNER TO django;

--
-- Name: audio_audio_panel_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.audio_audio_panel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.audio_audio_panel_id_seq OWNER TO django;

--
-- Name: audio_audio_panel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.audio_audio_panel_id_seq OWNED BY public.audio_audio_panel.id;


--
-- Name: audio_audioevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.audio_audioevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source character varying(10) NOT NULL,
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    judges text,
    sha1 character varying(40) NOT NULL,
    download_url character varying(500),
    local_path_mp3 character varying(100) NOT NULL,
    local_path_original_file character varying(100) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    duration smallint,
    processing_complete boolean NOT NULL,
    date_blocked date,
    blocked boolean NOT NULL,
    stt_status smallint NOT NULL,
    docket_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    stt_source smallint,
    stt_transcript text NOT NULL
);


ALTER TABLE public.audio_audioevent OWNER TO django;

--
-- Name: audio_audioevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.audio_audioevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.audio_audioevent_pgh_id_seq OWNER TO django;

--
-- Name: audio_audioevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.audio_audioevent_pgh_id_seq OWNED BY public.audio_audioevent.pgh_id;


--
-- Name: audio_audiopanelevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.audio_audiopanelevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    audio_id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid
);


ALTER TABLE public.audio_audiopanelevent OWNER TO django;

--
-- Name: audio_audiopanelevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.audio_audiopanelevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.audio_audiopanelevent_pgh_id_seq OWNER TO django;

--
-- Name: audio_audiopanelevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.audio_audiopanelevent_pgh_id_seq OWNED BY public.audio_audiopanelevent.pgh_id;


--
-- Name: audio_audiotranscriptionmetadata; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.audio_audiotranscriptionmetadata (
    id integer NOT NULL,
    metadata jsonb NOT NULL,
    audio_id integer NOT NULL
);


ALTER TABLE public.audio_audiotranscriptionmetadata OWNER TO django;

--
-- Name: audio_audiotranscriptionmetadata_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.audio_audiotranscriptionmetadata ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.audio_audiotranscriptionmetadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: disclosures_agreement; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_agreement (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_raw text NOT NULL,
    parties_and_terms text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_agreement OWNER TO django;

--
-- Name: disclosures_agreement_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_agreement_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_agreement_id_seq OWNER TO django;

--
-- Name: disclosures_agreement_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_agreement_id_seq OWNED BY public.disclosures_agreement.id;


--
-- Name: disclosures_agreementevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_agreementevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_raw text NOT NULL,
    parties_and_terms text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_agreementevent OWNER TO django;

--
-- Name: disclosures_agreementevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_agreementevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_agreementevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_agreementevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_agreementevent_pgh_id_seq OWNED BY public.disclosures_agreementevent.pgh_id;


--
-- Name: disclosures_debt; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_debt (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    creditor_name text NOT NULL,
    description text NOT NULL,
    value_code character varying(5) NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_debt OWNER TO django;

--
-- Name: disclosures_debt_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_debt_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_debt_id_seq OWNER TO django;

--
-- Name: disclosures_debt_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_debt_id_seq OWNED BY public.disclosures_debt.id;


--
-- Name: disclosures_debtevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_debtevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    creditor_name text NOT NULL,
    description text NOT NULL,
    value_code character varying(5) NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_debtevent OWNER TO django;

--
-- Name: disclosures_debtevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_debtevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_debtevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_debtevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_debtevent_pgh_id_seq OWNED BY public.disclosures_debtevent.pgh_id;


--
-- Name: disclosures_financialdisclosure; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_financialdisclosure (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    year smallint NOT NULL,
    download_filepath text NOT NULL,
    filepath character varying(300) NOT NULL,
    thumbnail character varying(300),
    thumbnail_status smallint NOT NULL,
    page_count smallint NOT NULL,
    sha1 character varying(40) NOT NULL,
    report_type smallint NOT NULL,
    is_amended boolean,
    addendum_content_raw text NOT NULL,
    addendum_redacted boolean NOT NULL,
    has_been_extracted boolean NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.disclosures_financialdisclosure OWNER TO django;

--
-- Name: disclosures_financialdisclosure_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_financialdisclosure_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_financialdisclosure_id_seq OWNER TO django;

--
-- Name: disclosures_financialdisclosure_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_financialdisclosure_id_seq OWNED BY public.disclosures_financialdisclosure.id;


--
-- Name: disclosures_financialdisclosureevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_financialdisclosureevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    year smallint NOT NULL,
    download_filepath text NOT NULL,
    filepath character varying(300) NOT NULL,
    thumbnail character varying(300),
    thumbnail_status smallint NOT NULL,
    page_count smallint NOT NULL,
    sha1 character varying(40) NOT NULL,
    report_type smallint NOT NULL,
    is_amended boolean,
    addendum_content_raw text NOT NULL,
    addendum_redacted boolean NOT NULL,
    has_been_extracted boolean NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_financialdisclosureevent OWNER TO django;

--
-- Name: disclosures_financialdisclosureevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_financialdisclosureevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_financialdisclosureevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_financialdisclosureevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_financialdisclosureevent_pgh_id_seq OWNED BY public.disclosures_financialdisclosureevent.pgh_id;


--
-- Name: disclosures_gift; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_gift (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source text NOT NULL,
    description text NOT NULL,
    value text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_gift OWNER TO django;

--
-- Name: disclosures_gift_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_gift_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_gift_id_seq OWNER TO django;

--
-- Name: disclosures_gift_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_gift_id_seq OWNED BY public.disclosures_gift.id;


--
-- Name: disclosures_giftevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_giftevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source text NOT NULL,
    description text NOT NULL,
    value text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_giftevent OWNER TO django;

--
-- Name: disclosures_giftevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_giftevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_giftevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_giftevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_giftevent_pgh_id_seq OWNED BY public.disclosures_giftevent.pgh_id;


--
-- Name: disclosures_investment; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_investment (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    page_number integer NOT NULL,
    description text NOT NULL,
    redacted boolean NOT NULL,
    income_during_reporting_period_code character varying(5) NOT NULL,
    income_during_reporting_period_type text NOT NULL,
    gross_value_code character varying(5) NOT NULL,
    gross_value_method character varying(5) NOT NULL,
    transaction_during_reporting_period text NOT NULL,
    transaction_date_raw character varying(40) NOT NULL,
    transaction_date date,
    transaction_value_code character varying(5) NOT NULL,
    transaction_gain_code character varying(5) NOT NULL,
    transaction_partner text NOT NULL,
    has_inferred_values boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_investment OWNER TO django;

--
-- Name: disclosures_investment_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_investment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_investment_id_seq OWNER TO django;

--
-- Name: disclosures_investment_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_investment_id_seq OWNED BY public.disclosures_investment.id;


--
-- Name: disclosures_investmentevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_investmentevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    page_number integer NOT NULL,
    description text NOT NULL,
    redacted boolean NOT NULL,
    income_during_reporting_period_code character varying(5) NOT NULL,
    income_during_reporting_period_type text NOT NULL,
    gross_value_code character varying(5) NOT NULL,
    gross_value_method character varying(5) NOT NULL,
    transaction_during_reporting_period text NOT NULL,
    transaction_date_raw character varying(40) NOT NULL,
    transaction_date date,
    transaction_value_code character varying(5) NOT NULL,
    transaction_gain_code character varying(5) NOT NULL,
    transaction_partner text NOT NULL,
    has_inferred_values boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_investmentevent OWNER TO django;

--
-- Name: disclosures_investmentevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_investmentevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_investmentevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_investmentevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_investmentevent_pgh_id_seq OWNED BY public.disclosures_investmentevent.pgh_id;


--
-- Name: disclosures_noninvestmentincome; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_noninvestmentincome (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_raw text NOT NULL,
    source_type text NOT NULL,
    income_amount text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_noninvestmentincome OWNER TO django;

--
-- Name: disclosures_noninvestmentincome_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_noninvestmentincome_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_noninvestmentincome_id_seq OWNER TO django;

--
-- Name: disclosures_noninvestmentincome_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_noninvestmentincome_id_seq OWNED BY public.disclosures_noninvestmentincome.id;


--
-- Name: disclosures_noninvestmentincomeevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_noninvestmentincomeevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_raw text NOT NULL,
    source_type text NOT NULL,
    income_amount text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_noninvestmentincomeevent OWNER TO django;

--
-- Name: disclosures_noninvestmentincomeevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_noninvestmentincomeevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_noninvestmentincomeevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_noninvestmentincomeevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_noninvestmentincomeevent_pgh_id_seq OWNED BY public.disclosures_noninvestmentincomeevent.pgh_id;


--
-- Name: disclosures_position; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_position (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    "position" text NOT NULL,
    organization_name text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_position OWNER TO django;

--
-- Name: disclosures_position_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_position_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_position_id_seq OWNER TO django;

--
-- Name: disclosures_position_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_position_id_seq OWNED BY public.disclosures_position.id;


--
-- Name: disclosures_positionevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_positionevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    "position" text NOT NULL,
    organization_name text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_positionevent OWNER TO django;

--
-- Name: disclosures_positionevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_positionevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_positionevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_positionevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_positionevent_pgh_id_seq OWNED BY public.disclosures_positionevent.pgh_id;


--
-- Name: disclosures_reimbursement; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_reimbursement (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source text NOT NULL,
    date_raw text NOT NULL,
    location text NOT NULL,
    purpose text NOT NULL,
    items_paid_or_provided text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_reimbursement OWNER TO django;

--
-- Name: disclosures_reimbursement_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_reimbursement_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_reimbursement_id_seq OWNER TO django;

--
-- Name: disclosures_reimbursement_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_reimbursement_id_seq OWNED BY public.disclosures_reimbursement.id;


--
-- Name: disclosures_reimbursementevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_reimbursementevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source text NOT NULL,
    date_raw text NOT NULL,
    location text NOT NULL,
    purpose text NOT NULL,
    items_paid_or_provided text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_reimbursementevent OWNER TO django;

--
-- Name: disclosures_reimbursementevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_reimbursementevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_reimbursementevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_reimbursementevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_reimbursementevent_pgh_id_seq OWNED BY public.disclosures_reimbursementevent.pgh_id;


--
-- Name: disclosures_spouseincome; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_spouseincome (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source_type text NOT NULL,
    date_raw text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL
);


ALTER TABLE public.disclosures_spouseincome OWNER TO django;

--
-- Name: disclosures_spouseincome_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_spouseincome_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_spouseincome_id_seq OWNER TO django;

--
-- Name: disclosures_spouseincome_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_spouseincome_id_seq OWNED BY public.disclosures_spouseincome.id;


--
-- Name: disclosures_spouseincomeevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.disclosures_spouseincomeevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source_type text NOT NULL,
    date_raw text NOT NULL,
    redacted boolean NOT NULL,
    financial_disclosure_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.disclosures_spouseincomeevent OWNER TO django;

--
-- Name: disclosures_spouseincomeevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.disclosures_spouseincomeevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.disclosures_spouseincomeevent_pgh_id_seq OWNER TO django;

--
-- Name: disclosures_spouseincomeevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.disclosures_spouseincomeevent_pgh_id_seq OWNED BY public.disclosures_spouseincomeevent.pgh_id;


--
-- Name: people_db_abarating; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_abarating (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    rating character varying(5) NOT NULL,
    person_id integer,
    year_rated smallint,
    CONSTRAINT people_db_abarating_year_rated_check CHECK ((year_rated >= 0))
);


ALTER TABLE public.people_db_abarating OWNER TO django;

--
-- Name: people_db_abarating_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_abarating_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_abarating_id_seq OWNER TO django;

--
-- Name: people_db_abarating_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_abarating_id_seq OWNED BY public.people_db_abarating.id;


--
-- Name: people_db_abaratingevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_abaratingevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    year_rated smallint,
    rating character varying(5) NOT NULL,
    person_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    CONSTRAINT people_db_abaratingevent_year_rated_check CHECK ((year_rated >= 0))
);


ALTER TABLE public.people_db_abaratingevent OWNER TO django;

--
-- Name: people_db_abaratingevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_abaratingevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_abaratingevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_abaratingevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_abaratingevent_pgh_id_seq OWNED BY public.people_db_abaratingevent.pgh_id;


--
-- Name: people_db_attorney; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_attorney (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name text NOT NULL,
    contact_raw text NOT NULL,
    phone character varying(20) NOT NULL,
    fax character varying(20) NOT NULL,
    email character varying(254) NOT NULL
);


ALTER TABLE public.people_db_attorney OWNER TO django;

--
-- Name: people_db_attorney_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_attorney_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_attorney_id_seq OWNER TO django;

--
-- Name: people_db_attorney_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_attorney_id_seq OWNED BY public.people_db_attorney.id;


--
-- Name: people_db_attorneyorganization; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_attorneyorganization (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    lookup_key text NOT NULL,
    name text NOT NULL,
    address1 text NOT NULL,
    address2 text NOT NULL,
    city text NOT NULL,
    state character varying(2) NOT NULL,
    zip_code character varying(10) NOT NULL
);


ALTER TABLE public.people_db_attorneyorganization OWNER TO django;

--
-- Name: people_db_attorneyorganization_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_attorneyorganization_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_attorneyorganization_id_seq OWNER TO django;

--
-- Name: people_db_attorneyorganization_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_attorneyorganization_id_seq OWNED BY public.people_db_attorneyorganization.id;


--
-- Name: people_db_attorneyorganizationassociation; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_attorneyorganizationassociation (
    id integer NOT NULL,
    attorney_id integer NOT NULL,
    attorney_organization_id integer NOT NULL,
    docket_id integer NOT NULL
);


ALTER TABLE public.people_db_attorneyorganizationassociation OWNER TO django;

--
-- Name: people_db_attorneyorganizationassociation_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_attorneyorganizationassociation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_attorneyorganizationassociation_id_seq OWNER TO django;

--
-- Name: people_db_attorneyorganizationassociation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_attorneyorganizationassociation_id_seq OWNED BY public.people_db_attorneyorganizationassociation.id;


--
-- Name: people_db_criminalcomplaint; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_criminalcomplaint (
    id integer NOT NULL,
    name text NOT NULL,
    disposition text NOT NULL,
    party_type_id integer NOT NULL
);


ALTER TABLE public.people_db_criminalcomplaint OWNER TO django;

--
-- Name: people_db_criminalcomplaint_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_criminalcomplaint_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_criminalcomplaint_id_seq OWNER TO django;

--
-- Name: people_db_criminalcomplaint_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_criminalcomplaint_id_seq OWNED BY public.people_db_criminalcomplaint.id;


--
-- Name: people_db_criminalcount; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_criminalcount (
    id integer NOT NULL,
    name text NOT NULL,
    disposition text NOT NULL,
    status smallint NOT NULL,
    party_type_id integer NOT NULL
);


ALTER TABLE public.people_db_criminalcount OWNER TO django;

--
-- Name: people_db_criminalcount_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_criminalcount_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_criminalcount_id_seq OWNER TO django;

--
-- Name: people_db_criminalcount_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_criminalcount_id_seq OWNED BY public.people_db_criminalcount.id;


--
-- Name: people_db_education; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_education (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    degree_detail character varying(100) NOT NULL,
    degree_year smallint,
    person_id integer,
    school_id integer NOT NULL,
    degree_level character varying(4) NOT NULL,
    CONSTRAINT people_db_education_degree_year_check CHECK ((degree_year >= 0))
);


ALTER TABLE public.people_db_education OWNER TO django;

--
-- Name: people_db_education_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_education_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_education_id_seq OWNER TO django;

--
-- Name: people_db_education_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_education_id_seq OWNED BY public.people_db_education.id;


--
-- Name: people_db_educationevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_educationevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    degree_level character varying(4) NOT NULL,
    degree_detail character varying(100) NOT NULL,
    degree_year smallint,
    person_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    school_id integer NOT NULL,
    CONSTRAINT people_db_educationevent_degree_year_check CHECK ((degree_year >= 0))
);


ALTER TABLE public.people_db_educationevent OWNER TO django;

--
-- Name: people_db_educationevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_educationevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_educationevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_educationevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_educationevent_pgh_id_seq OWNED BY public.people_db_educationevent.pgh_id;


--
-- Name: people_db_party; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_party (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name text NOT NULL,
    extra_info text NOT NULL
);


ALTER TABLE public.people_db_party OWNER TO django;

--
-- Name: people_db_party_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_party_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_party_id_seq OWNER TO django;

--
-- Name: people_db_party_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_party_id_seq OWNED BY public.people_db_party.id;


--
-- Name: people_db_partytype; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_partytype (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    docket_id integer NOT NULL,
    party_id integer NOT NULL,
    date_terminated date,
    extra_info text NOT NULL,
    highest_offense_level_opening text NOT NULL,
    highest_offense_level_terminated text NOT NULL
);


ALTER TABLE public.people_db_partytype OWNER TO django;

--
-- Name: people_db_partytype_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_partytype_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_partytype_id_seq OWNER TO django;

--
-- Name: people_db_partytype_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_partytype_id_seq OWNED BY public.people_db_partytype.id;


--
-- Name: people_db_person; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_person (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    fjc_id integer,
    slug character varying(158) NOT NULL,
    name_first character varying(50) NOT NULL,
    name_middle character varying(50) NOT NULL,
    name_last character varying(50) NOT NULL,
    name_suffix character varying(5) NOT NULL,
    date_dob date,
    date_granularity_dob character varying(15) NOT NULL,
    date_dod date,
    date_granularity_dod character varying(15) NOT NULL,
    dob_city character varying(50) NOT NULL,
    dob_state character varying(2) NOT NULL,
    dod_city character varying(50) NOT NULL,
    dod_state character varying(2) NOT NULL,
    gender character varying(2) NOT NULL,
    is_alias_of_id integer,
    religion character varying(30) NOT NULL,
    has_photo boolean NOT NULL,
    ftm_total_received double precision,
    ftm_eid character varying(30),
    date_completed timestamp with time zone,
    dob_country character varying(50) NOT NULL,
    dod_country character varying(50) NOT NULL
);


ALTER TABLE public.people_db_person OWNER TO django;

--
-- Name: people_db_person_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_person_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_person_id_seq OWNER TO django;

--
-- Name: people_db_person_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_person_id_seq OWNED BY public.people_db_person.id;


--
-- Name: people_db_person_race; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_person_race (
    id integer NOT NULL,
    person_id integer NOT NULL,
    race_id integer NOT NULL
);


ALTER TABLE public.people_db_person_race OWNER TO django;

--
-- Name: people_db_person_race_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_person_race_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_person_race_id_seq OWNER TO django;

--
-- Name: people_db_person_race_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_person_race_id_seq OWNED BY public.people_db_person_race.id;


--
-- Name: people_db_personevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_personevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_completed timestamp with time zone,
    fjc_id integer,
    slug character varying(158) NOT NULL,
    name_first character varying(50) NOT NULL,
    name_middle character varying(50) NOT NULL,
    name_last character varying(50) NOT NULL,
    name_suffix character varying(5) NOT NULL,
    date_dob date,
    date_granularity_dob character varying(15) NOT NULL,
    date_dod date,
    date_granularity_dod character varying(15) NOT NULL,
    dob_city character varying(50) NOT NULL,
    dob_state character varying(2) NOT NULL,
    dob_country character varying(50) NOT NULL,
    dod_city character varying(50) NOT NULL,
    dod_state character varying(2) NOT NULL,
    dod_country character varying(50) NOT NULL,
    gender character varying(2) NOT NULL,
    religion character varying(30) NOT NULL,
    ftm_total_received double precision,
    ftm_eid character varying(30),
    has_photo boolean NOT NULL,
    is_alias_of_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.people_db_personevent OWNER TO django;

--
-- Name: people_db_personevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_personevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_personevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_personevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_personevent_pgh_id_seq OWNED BY public.people_db_personevent.pgh_id;


--
-- Name: people_db_personraceevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_personraceevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid,
    race_id integer NOT NULL
);


ALTER TABLE public.people_db_personraceevent OWNER TO django;

--
-- Name: people_db_personraceevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_personraceevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_personraceevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_personraceevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_personraceevent_pgh_id_seq OWNED BY public.people_db_personraceevent.pgh_id;


--
-- Name: people_db_politicalaffiliation; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_politicalaffiliation (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    political_party character varying(5) NOT NULL,
    source character varying(5) NOT NULL,
    date_start date,
    date_granularity_start character varying(15) NOT NULL,
    date_end date,
    date_granularity_end character varying(15) NOT NULL,
    person_id integer
);


ALTER TABLE public.people_db_politicalaffiliation OWNER TO django;

--
-- Name: people_db_politicalaffiliation_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_politicalaffiliation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_politicalaffiliation_id_seq OWNER TO django;

--
-- Name: people_db_politicalaffiliation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_politicalaffiliation_id_seq OWNED BY public.people_db_politicalaffiliation.id;


--
-- Name: people_db_politicalaffiliationevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_politicalaffiliationevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    political_party character varying(5) NOT NULL,
    source character varying(5) NOT NULL,
    date_start date,
    date_granularity_start character varying(15) NOT NULL,
    date_end date,
    date_granularity_end character varying(15) NOT NULL,
    person_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.people_db_politicalaffiliationevent OWNER TO django;

--
-- Name: people_db_politicalaffiliationevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_politicalaffiliationevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_politicalaffiliationevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_politicalaffiliationevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_politicalaffiliationevent_pgh_id_seq OWNED BY public.people_db_politicalaffiliationevent.pgh_id;


--
-- Name: people_db_position; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_position (
    id integer NOT NULL,
    position_type character varying(20),
    job_title character varying(100) NOT NULL,
    organization_name character varying(120),
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_nominated date,
    date_elected date,
    date_recess_appointment date,
    date_referred_to_judicial_committee date,
    date_judicial_committee_action date,
    date_hearing date,
    date_confirmation date,
    date_start date,
    date_granularity_start character varying(15) NOT NULL,
    date_retirement date,
    date_termination date,
    date_granularity_termination character varying(15) NOT NULL,
    judicial_committee_action character varying(20) NOT NULL,
    nomination_process character varying(20) NOT NULL,
    voice_vote boolean,
    votes_yes integer,
    votes_no integer,
    how_selected character varying(20) NOT NULL,
    termination_reason character varying(25) NOT NULL,
    court_id character varying(15),
    person_id integer,
    school_id integer,
    appointer_id integer,
    predecessor_id integer,
    supervisor_id integer,
    vote_type character varying(2) NOT NULL,
    votes_no_percent double precision,
    votes_yes_percent double precision,
    location_city character varying(50) NOT NULL,
    location_state character varying(2) NOT NULL,
    has_inferred_values boolean NOT NULL,
    sector smallint,
    CONSTRAINT people_db_position_votes_no_check CHECK ((votes_no >= 0)),
    CONSTRAINT people_db_position_votes_yes_check CHECK ((votes_yes >= 0))
);


ALTER TABLE public.people_db_position OWNER TO django;

--
-- Name: people_db_position_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_position_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_position_id_seq OWNER TO django;

--
-- Name: people_db_position_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_position_id_seq OWNED BY public.people_db_position.id;


--
-- Name: people_db_positionevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_positionevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    position_type character varying(20),
    job_title character varying(100) NOT NULL,
    sector smallint,
    organization_name character varying(120),
    location_city character varying(50) NOT NULL,
    location_state character varying(2) NOT NULL,
    date_nominated date,
    date_elected date,
    date_recess_appointment date,
    date_referred_to_judicial_committee date,
    date_judicial_committee_action date,
    judicial_committee_action character varying(20) NOT NULL,
    date_hearing date,
    date_confirmation date,
    date_start date,
    date_granularity_start character varying(15) NOT NULL,
    date_termination date,
    termination_reason character varying(25) NOT NULL,
    date_granularity_termination character varying(15) NOT NULL,
    date_retirement date,
    nomination_process character varying(20) NOT NULL,
    vote_type character varying(2) NOT NULL,
    voice_vote boolean,
    votes_yes integer,
    votes_no integer,
    votes_yes_percent double precision,
    votes_no_percent double precision,
    how_selected character varying(20) NOT NULL,
    has_inferred_values boolean NOT NULL,
    appointer_id integer,
    court_id character varying(15),
    person_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    predecessor_id integer,
    school_id integer,
    supervisor_id integer,
    CONSTRAINT people_db_positionevent_votes_no_check CHECK ((votes_no >= 0)),
    CONSTRAINT people_db_positionevent_votes_yes_check CHECK ((votes_yes >= 0))
);


ALTER TABLE public.people_db_positionevent OWNER TO django;

--
-- Name: people_db_positionevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_positionevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_positionevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_positionevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_positionevent_pgh_id_seq OWNED BY public.people_db_positionevent.pgh_id;


--
-- Name: people_db_race; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_race (
    id integer NOT NULL,
    race character varying(5) NOT NULL
);


ALTER TABLE public.people_db_race OWNER TO django;

--
-- Name: people_db_race_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_race_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_race_id_seq OWNER TO django;

--
-- Name: people_db_race_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_race_id_seq OWNED BY public.people_db_race.id;


--
-- Name: people_db_raceevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_raceevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    race character varying(5) NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.people_db_raceevent OWNER TO django;

--
-- Name: people_db_raceevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_raceevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_raceevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_raceevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_raceevent_pgh_id_seq OWNED BY public.people_db_raceevent.pgh_id;


--
-- Name: people_db_retentionevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_retentionevent (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    retention_type character varying(10) NOT NULL,
    date_retention date NOT NULL,
    votes_yes integer,
    votes_no integer,
    unopposed boolean,
    won boolean,
    position_id integer,
    votes_no_percent double precision,
    votes_yes_percent double precision,
    CONSTRAINT people_db_retentionevent_votes_no_check CHECK ((votes_no >= 0)),
    CONSTRAINT people_db_retentionevent_votes_yes_check CHECK ((votes_yes >= 0))
);


ALTER TABLE public.people_db_retentionevent OWNER TO django;

--
-- Name: people_db_retentionevent_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_retentionevent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_retentionevent_id_seq OWNER TO django;

--
-- Name: people_db_retentionevent_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_retentionevent_id_seq OWNED BY public.people_db_retentionevent.id;


--
-- Name: people_db_retentioneventevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_retentioneventevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    retention_type character varying(10) NOT NULL,
    date_retention date NOT NULL,
    votes_yes integer,
    votes_no integer,
    votes_yes_percent double precision,
    votes_no_percent double precision,
    unopposed boolean,
    won boolean,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    position_id integer,
    CONSTRAINT people_db_retentioneventevent_votes_no_check CHECK ((votes_no >= 0)),
    CONSTRAINT people_db_retentioneventevent_votes_yes_check CHECK ((votes_yes >= 0))
);


ALTER TABLE public.people_db_retentioneventevent OWNER TO django;

--
-- Name: people_db_retentioneventevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_retentioneventevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_retentioneventevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_retentioneventevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_retentioneventevent_pgh_id_seq OWNED BY public.people_db_retentioneventevent.pgh_id;


--
-- Name: people_db_role; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_role (
    id integer NOT NULL,
    role smallint,
    date_action date,
    attorney_id integer NOT NULL,
    docket_id integer NOT NULL,
    party_id integer NOT NULL,
    role_raw text NOT NULL
);


ALTER TABLE public.people_db_role OWNER TO django;

--
-- Name: people_db_role_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_role_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_role_id_seq OWNER TO django;

--
-- Name: people_db_role_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_role_id_seq OWNED BY public.people_db_role.id;


--
-- Name: people_db_school; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_school (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name character varying(120) NOT NULL,
    ein integer,
    is_alias_of_id integer
);


ALTER TABLE public.people_db_school OWNER TO django;

--
-- Name: people_db_school_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_school_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_school_id_seq OWNER TO django;

--
-- Name: people_db_school_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_school_id_seq OWNED BY public.people_db_school.id;


--
-- Name: people_db_schoolevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_schoolevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name character varying(120) NOT NULL,
    ein integer,
    is_alias_of_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.people_db_schoolevent OWNER TO django;

--
-- Name: people_db_schoolevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_schoolevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_schoolevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_schoolevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_schoolevent_pgh_id_seq OWNED BY public.people_db_schoolevent.pgh_id;


--
-- Name: people_db_source; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_source (
    id integer NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    url character varying(2000) NOT NULL,
    date_accessed date,
    notes text NOT NULL,
    person_id integer,
    date_created timestamp with time zone NOT NULL
);


ALTER TABLE public.people_db_source OWNER TO django;

--
-- Name: people_db_source_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_source_id_seq OWNER TO django;

--
-- Name: people_db_source_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_source_id_seq OWNED BY public.people_db_source.id;


--
-- Name: people_db_sourceevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.people_db_sourceevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    url character varying(2000) NOT NULL,
    date_accessed date,
    notes text NOT NULL,
    person_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.people_db_sourceevent OWNER TO django;

--
-- Name: people_db_sourceevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.people_db_sourceevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.people_db_sourceevent_pgh_id_seq OWNER TO django;

--
-- Name: people_db_sourceevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.people_db_sourceevent_pgh_id_seq OWNED BY public.people_db_sourceevent.pgh_id;


--
-- Name: recap_emailprocessingqueue; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_emailprocessingqueue (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    filepath character varying(300),
    status smallint NOT NULL,
    status_message text NOT NULL,
    court_id character varying(15) NOT NULL,
    uploader_id integer NOT NULL,
    message_id text NOT NULL,
    destination_emails jsonb NOT NULL
);


ALTER TABLE public.recap_emailprocessingqueue OWNER TO django;

--
-- Name: recap_emailprocessingqueue_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_emailprocessingqueue_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_emailprocessingqueue_id_seq OWNER TO django;

--
-- Name: recap_emailprocessingqueue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_emailprocessingqueue_id_seq OWNED BY public.recap_emailprocessingqueue.id;


--
-- Name: recap_emailprocessingqueue_recap_documents; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_emailprocessingqueue_recap_documents (
    id integer NOT NULL,
    emailprocessingqueue_id integer NOT NULL,
    recapdocument_id integer NOT NULL
);


ALTER TABLE public.recap_emailprocessingqueue_recap_documents OWNER TO django;

--
-- Name: recap_emailprocessingqueue_recap_documents_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_emailprocessingqueue_recap_documents_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_emailprocessingqueue_recap_documents_id_seq OWNER TO django;

--
-- Name: recap_emailprocessingqueue_recap_documents_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_emailprocessingqueue_recap_documents_id_seq OWNED BY public.recap_emailprocessingqueue_recap_documents.id;


--
-- Name: recap_fjcintegrateddatabase; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_fjcintegrateddatabase (
    id integer NOT NULL,
    dataset_source smallint NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    office character varying(3) NOT NULL,
    docket_number character varying(7) NOT NULL,
    origin smallint,
    date_filed date,
    jurisdiction smallint,
    nature_of_suit integer,
    title text NOT NULL,
    section character varying(200) NOT NULL,
    subsection character varying(200) NOT NULL,
    diversity_of_residence smallint,
    class_action boolean,
    monetary_demand integer,
    county_of_residence integer,
    arbitration_at_filing character varying(1) NOT NULL,
    arbitration_at_termination character varying(1) NOT NULL,
    multidistrict_litigation_docket_number text NOT NULL,
    plaintiff text NOT NULL,
    defendant text NOT NULL,
    date_transfer date,
    transfer_office character varying(3) NOT NULL,
    transfer_docket_number text NOT NULL,
    transfer_origin text NOT NULL,
    date_terminated date,
    termination_class_action_status smallint,
    procedural_progress smallint,
    disposition smallint,
    nature_of_judgement smallint,
    amount_received integer,
    judgment smallint,
    pro_se smallint,
    year_of_tape integer,
    circuit_id character varying(15),
    district_id character varying(15),
    nature_of_offense character varying(4) NOT NULL,
    version integer
);


ALTER TABLE public.recap_fjcintegrateddatabase OWNER TO django;

--
-- Name: recap_fjcintegrateddatabase_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_fjcintegrateddatabase_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_fjcintegrateddatabase_id_seq OWNER TO django;

--
-- Name: recap_fjcintegrateddatabase_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_fjcintegrateddatabase_id_seq OWNED BY public.recap_fjcintegrateddatabase.id;


--
-- Name: recap_pacerfetchqueue; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_pacerfetchqueue (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_completed timestamp with time zone,
    status smallint NOT NULL,
    request_type smallint NOT NULL,
    message text NOT NULL,
    pacer_case_id character varying(100) NOT NULL,
    docket_number character varying(50) NOT NULL,
    de_date_start date,
    de_date_end date,
    de_number_start integer,
    de_number_end integer,
    show_parties_and_counsel boolean NOT NULL,
    show_terminated_parties boolean NOT NULL,
    show_list_of_member_cases boolean NOT NULL,
    court_id character varying(15),
    docket_id integer,
    recap_document_id integer,
    user_id integer NOT NULL
);


ALTER TABLE public.recap_pacerfetchqueue OWNER TO django;

--
-- Name: recap_pacerfetchqueue_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_pacerfetchqueue_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_pacerfetchqueue_id_seq OWNER TO django;

--
-- Name: recap_pacerfetchqueue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_pacerfetchqueue_id_seq OWNED BY public.recap_pacerfetchqueue.id;


--
-- Name: recap_pacerhtmlfiles; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_pacerhtmlfiles (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    filepath character varying(150) NOT NULL,
    object_id integer NOT NULL,
    content_type_id integer NOT NULL,
    upload_type smallint NOT NULL,
    CONSTRAINT recap_pacerhtmlfiles_object_id_check CHECK ((object_id >= 0))
);


ALTER TABLE public.recap_pacerhtmlfiles OWNER TO django;

--
-- Name: recap_pacerhtmlfiles_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_pacerhtmlfiles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_pacerhtmlfiles_id_seq OWNER TO django;

--
-- Name: recap_pacerhtmlfiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_pacerhtmlfiles_id_seq OWNED BY public.recap_pacerhtmlfiles.id;


--
-- Name: recap_processingqueue; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_processingqueue (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    pacer_case_id character varying(100) NOT NULL,
    pacer_doc_id character varying(64) NOT NULL,
    attachment_number smallint,
    filepath_local character varying(1000) NOT NULL,
    status smallint NOT NULL,
    upload_type smallint NOT NULL,
    error_message text NOT NULL,
    court_id character varying(15) NOT NULL,
    uploader_id integer NOT NULL,
    docket_id integer,
    docket_entry_id integer,
    recap_document_id integer,
    document_number bigint,
    debug boolean NOT NULL,
    acms_document_guid character varying(64) NOT NULL
);


ALTER TABLE public.recap_processingqueue OWNER TO django;

--
-- Name: recap_processingqueue_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_processingqueue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_processingqueue_id_seq OWNER TO django;

--
-- Name: recap_processingqueue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_processingqueue_id_seq OWNED BY public.recap_processingqueue.id;


--
-- Name: recap_rss_rssfeeddata; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_rss_rssfeeddata (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    filepath character varying(150) NOT NULL,
    court_id character varying(15) NOT NULL
);


ALTER TABLE public.recap_rss_rssfeeddata OWNER TO django;

--
-- Name: recap_rss_rssfeeddata_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_rss_rssfeeddata_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_rss_rssfeeddata_id_seq OWNER TO django;

--
-- Name: recap_rss_rssfeeddata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_rss_rssfeeddata_id_seq OWNED BY public.recap_rss_rssfeeddata.id;


--
-- Name: recap_rss_rssfeedstatus; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_rss_rssfeedstatus (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_last_build timestamp with time zone,
    status smallint NOT NULL,
    court_id character varying(15) NOT NULL,
    is_sweep boolean NOT NULL
);


ALTER TABLE public.recap_rss_rssfeedstatus OWNER TO django;

--
-- Name: recap_rss_rssfeedstatus_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_rss_rssfeedstatus_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_rss_rssfeedstatus_id_seq OWNER TO django;

--
-- Name: recap_rss_rssfeedstatus_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_rss_rssfeedstatus_id_seq OWNED BY public.recap_rss_rssfeedstatus.id;


--
-- Name: recap_rss_rssitemcache; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.recap_rss_rssitemcache (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    hash character varying(64) NOT NULL
);


ALTER TABLE public.recap_rss_rssitemcache OWNER TO django;

--
-- Name: recap_rss_rssitemcache_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.recap_rss_rssitemcache_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.recap_rss_rssitemcache_id_seq OWNER TO django;

--
-- Name: recap_rss_rssitemcache_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.recap_rss_rssitemcache_id_seq OWNED BY public.recap_rss_rssitemcache.id;


--
-- Name: search_court; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_court (
    id character varying(15) NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    in_use boolean NOT NULL,
    has_opinion_scraper boolean NOT NULL,
    has_oral_argument_scraper boolean NOT NULL,
    "position" double precision NOT NULL,
    citation_string character varying(100) NOT NULL,
    short_name character varying(100) NOT NULL,
    full_name character varying(200) NOT NULL,
    url character varying(500) NOT NULL,
    start_date date,
    end_date date,
    jurisdiction character varying(3) NOT NULL,
    notes text NOT NULL,
    pacer_court_id smallint,
    fjc_court_id character varying(3) NOT NULL,
    pacer_has_rss_feed boolean,
    date_last_pacer_contact timestamp with time zone,
    pacer_rss_entry_types text NOT NULL,
    parent_court_id character varying(15),
    CONSTRAINT search_court_pacer_court_id_check CHECK ((pacer_court_id >= 0))
);


ALTER TABLE public.search_court OWNER TO django;

--
-- Name: search_docket; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docket (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_argued date,
    date_reargued date,
    date_reargument_denied date,
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    slug character varying(75) NOT NULL,
    docket_number text,
    date_blocked date,
    blocked boolean NOT NULL,
    court_id character varying(15) NOT NULL,
    date_cert_denied date,
    date_cert_granted date,
    assigned_to_id integer,
    cause character varying(2000) NOT NULL,
    date_filed date,
    date_last_filing date,
    date_terminated date,
    filepath_ia character varying(1000) NOT NULL,
    filepath_local character varying(1000) NOT NULL,
    jurisdiction_type character varying(100) NOT NULL,
    jury_demand character varying(500) NOT NULL,
    nature_of_suit character varying(1000) NOT NULL,
    pacer_case_id character varying(100),
    referred_to_id integer,
    source smallint NOT NULL,
    assigned_to_str text NOT NULL,
    referred_to_str text NOT NULL,
    view_count integer NOT NULL,
    date_last_index timestamp with time zone,
    appeal_from_id character varying(15),
    appeal_from_str text NOT NULL,
    appellate_case_type_information text NOT NULL,
    appellate_fee_status text NOT NULL,
    panel_str text NOT NULL,
    originating_court_information_id integer,
    mdl_status character varying(100) NOT NULL,
    filepath_ia_json character varying(1000) NOT NULL,
    ia_date_first_change timestamp with time zone,
    ia_needs_upload boolean,
    ia_upload_failure_count smallint,
    docket_number_core character varying(20) NOT NULL,
    idb_data_id integer,
    federal_defendant_number smallint,
    federal_dn_case_type character varying(6) NOT NULL,
    federal_dn_judge_initials_assigned character varying(5) NOT NULL,
    federal_dn_judge_initials_referred character varying(5) NOT NULL,
    federal_dn_office_code character varying(3) NOT NULL,
    parent_docket_id integer
);


ALTER TABLE public.search_docket OWNER TO django;

--
-- Name: search_opinion; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinion (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    type character varying(20) NOT NULL,
    sha1 character varying(40) NOT NULL,
    download_url character varying(500),
    local_path character varying(100) NOT NULL,
    plain_text text NOT NULL,
    html text NOT NULL,
    html_lawbox text NOT NULL,
    html_columbia text NOT NULL,
    html_with_citations text NOT NULL,
    extracted_by_ocr boolean NOT NULL,
    author_id integer,
    cluster_id integer NOT NULL,
    per_curiam boolean NOT NULL,
    page_count integer,
    author_str text NOT NULL,
    joined_by_str text NOT NULL,
    xml_harvard text NOT NULL,
    html_anon_2020 text NOT NULL,
    ordering_key integer
);


ALTER TABLE public.search_opinion OWNER TO django;

--
-- Name: search_opinioncluster; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinioncluster (
    id integer NOT NULL,
    judges text NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_filed date NOT NULL,
    slug character varying(75),
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    scdb_id character varying(10) NOT NULL,
    source character varying(10) NOT NULL,
    procedural_history text NOT NULL,
    attorneys text NOT NULL,
    nature_of_suit text NOT NULL,
    posture text NOT NULL,
    syllabus text NOT NULL,
    citation_count integer NOT NULL,
    precedential_status character varying(50) NOT NULL,
    date_blocked date,
    blocked boolean NOT NULL,
    docket_id integer NOT NULL,
    scdb_decision_direction integer,
    scdb_votes_majority integer,
    scdb_votes_minority integer,
    date_filed_is_approximate boolean NOT NULL,
    correction text NOT NULL,
    cross_reference text NOT NULL,
    disposition text NOT NULL,
    filepath_json_harvard character varying(1000) NOT NULL,
    headnotes text NOT NULL,
    history text NOT NULL,
    other_dates text NOT NULL,
    summary text NOT NULL,
    arguments text NOT NULL,
    headmatter text NOT NULL,
    filepath_pdf_harvard character varying(100) NOT NULL
);


ALTER TABLE public.search_opinioncluster OWNER TO django;

--
-- Name: search_bankruptcyinformation; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_bankruptcyinformation (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_converted timestamp with time zone,
    date_last_to_file_claims timestamp with time zone,
    date_last_to_file_govt timestamp with time zone,
    date_debtor_dismissed timestamp with time zone,
    chapter character varying(10) NOT NULL,
    trustee_str text NOT NULL,
    docket_id integer NOT NULL
);


ALTER TABLE public.search_bankruptcyinformation OWNER TO django;

--
-- Name: search_bankruptcyinformation_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_bankruptcyinformation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_bankruptcyinformation_id_seq OWNER TO django;

--
-- Name: search_bankruptcyinformation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_bankruptcyinformation_id_seq OWNED BY public.search_bankruptcyinformation.id;


--
-- Name: search_bankruptcyinformationevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_bankruptcyinformationevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_converted timestamp with time zone,
    date_last_to_file_claims timestamp with time zone,
    date_last_to_file_govt timestamp with time zone,
    date_debtor_dismissed timestamp with time zone,
    chapter character varying(10) NOT NULL,
    trustee_str text NOT NULL,
    docket_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_bankruptcyinformationevent OWNER TO django;

--
-- Name: search_bankruptcyinformationevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_bankruptcyinformationevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_bankruptcyinformationevent_pgh_id_seq OWNER TO django;

--
-- Name: search_bankruptcyinformationevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_bankruptcyinformationevent_pgh_id_seq OWNED BY public.search_bankruptcyinformationevent.pgh_id;


--
-- Name: search_citation; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_citation (
    id integer NOT NULL,
    volume smallint NOT NULL,
    reporter text NOT NULL,
    page text NOT NULL,
    type smallint NOT NULL,
    cluster_id integer NOT NULL
);


ALTER TABLE public.search_citation OWNER TO django;

--
-- Name: search_citation_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_citation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_citation_id_seq OWNER TO django;

--
-- Name: search_citation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_citation_id_seq OWNED BY public.search_citation.id;


--
-- Name: search_citationevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_citationevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    volume smallint NOT NULL,
    reporter text NOT NULL,
    page text NOT NULL,
    type smallint NOT NULL,
    cluster_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_citationevent OWNER TO django;

--
-- Name: search_citationevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_citationevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_citationevent_pgh_id_seq OWNER TO django;

--
-- Name: search_citationevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_citationevent_pgh_id_seq OWNED BY public.search_citationevent.pgh_id;


--
-- Name: search_claim; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claim (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_claim_modified timestamp with time zone,
    date_original_entered timestamp with time zone,
    date_original_filed timestamp with time zone,
    date_last_amendment_entered timestamp with time zone,
    date_last_amendment_filed timestamp with time zone,
    claim_number character varying(10) NOT NULL,
    creditor_details text NOT NULL,
    creditor_id character varying(50) NOT NULL,
    status character varying(1000) NOT NULL,
    entered_by character varying(1000) NOT NULL,
    filed_by character varying(1000) NOT NULL,
    amount_claimed character varying(100) NOT NULL,
    unsecured_claimed character varying(100) NOT NULL,
    secured_claimed character varying(100) NOT NULL,
    priority_claimed character varying(100) NOT NULL,
    description text NOT NULL,
    remarks text NOT NULL,
    docket_id integer NOT NULL
);


ALTER TABLE public.search_claim OWNER TO django;

--
-- Name: search_claim_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claim_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claim_id_seq OWNER TO django;

--
-- Name: search_claim_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claim_id_seq OWNED BY public.search_claim.id;


--
-- Name: search_claim_tags; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claim_tags (
    id integer NOT NULL,
    claim_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_claim_tags OWNER TO django;

--
-- Name: search_claim_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claim_tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claim_tags_id_seq OWNER TO django;

--
-- Name: search_claim_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claim_tags_id_seq OWNED BY public.search_claim_tags.id;


--
-- Name: search_claimevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claimevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_claim_modified timestamp with time zone,
    date_original_entered timestamp with time zone,
    date_original_filed timestamp with time zone,
    date_last_amendment_entered timestamp with time zone,
    date_last_amendment_filed timestamp with time zone,
    claim_number character varying(10) NOT NULL,
    creditor_details text NOT NULL,
    creditor_id character varying(50) NOT NULL,
    status character varying(1000) NOT NULL,
    entered_by character varying(1000) NOT NULL,
    filed_by character varying(1000) NOT NULL,
    amount_claimed character varying(100) NOT NULL,
    unsecured_claimed character varying(100) NOT NULL,
    secured_claimed character varying(100) NOT NULL,
    priority_claimed character varying(100) NOT NULL,
    description text NOT NULL,
    remarks text NOT NULL,
    docket_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_claimevent OWNER TO django;

--
-- Name: search_claimevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claimevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claimevent_pgh_id_seq OWNER TO django;

--
-- Name: search_claimevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claimevent_pgh_id_seq OWNED BY public.search_claimevent.pgh_id;


--
-- Name: search_claimhistory; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claimhistory (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_upload timestamp with time zone,
    document_number character varying(32) NOT NULL,
    attachment_number smallint,
    pacer_doc_id character varying(64) NOT NULL,
    is_available boolean,
    sha1 character varying(40) NOT NULL,
    page_count integer,
    file_size integer,
    filepath_local character varying(1000) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    thumbnail character varying(100),
    thumbnail_status smallint NOT NULL,
    plain_text text NOT NULL,
    ocr_status smallint,
    is_free_on_pacer boolean,
    is_sealed boolean,
    date_filed date,
    claim_document_type integer NOT NULL,
    description text NOT NULL,
    claim_doc_id character varying(32) NOT NULL,
    pacer_dm_id integer,
    pacer_case_id character varying(100) NOT NULL,
    claim_id integer NOT NULL
);


ALTER TABLE public.search_claimhistory OWNER TO django;

--
-- Name: search_claimhistory_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claimhistory_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claimhistory_id_seq OWNER TO django;

--
-- Name: search_claimhistory_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claimhistory_id_seq OWNED BY public.search_claimhistory.id;


--
-- Name: search_claimhistoryevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claimhistoryevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    sha1 character varying(40) NOT NULL,
    page_count integer,
    file_size integer,
    filepath_local character varying(1000) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    thumbnail character varying(100),
    thumbnail_status smallint NOT NULL,
    plain_text text NOT NULL,
    ocr_status smallint,
    date_upload timestamp with time zone,
    document_number character varying(32) NOT NULL,
    attachment_number smallint,
    pacer_doc_id character varying(64) NOT NULL,
    is_available boolean,
    is_free_on_pacer boolean,
    is_sealed boolean,
    date_filed date,
    claim_document_type integer NOT NULL,
    description text NOT NULL,
    claim_doc_id character varying(32) NOT NULL,
    pacer_dm_id integer,
    pacer_case_id character varying(100) NOT NULL,
    claim_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_claimhistoryevent OWNER TO django;

--
-- Name: search_claimhistoryevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claimhistoryevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claimhistoryevent_pgh_id_seq OWNER TO django;

--
-- Name: search_claimhistoryevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claimhistoryevent_pgh_id_seq OWNED BY public.search_claimhistoryevent.pgh_id;


--
-- Name: search_claimtagsevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_claimtagsevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    claim_id integer NOT NULL,
    pgh_context_id uuid,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_claimtagsevent OWNER TO django;

--
-- Name: search_claimtagsevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_claimtagsevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_claimtagsevent_pgh_id_seq OWNER TO django;

--
-- Name: search_claimtagsevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_claimtagsevent_pgh_id_seq OWNED BY public.search_claimtagsevent.pgh_id;


--
-- Name: search_court_appeals_to; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_court_appeals_to (
    id integer NOT NULL,
    from_court_id character varying(15) NOT NULL,
    to_court_id character varying(15) NOT NULL
);


ALTER TABLE public.search_court_appeals_to OWNER TO django;

--
-- Name: search_court_appeals_to_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.search_court_appeals_to ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.search_court_appeals_to_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: search_courtappealstoevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_courtappealstoevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    from_court_id character varying(15) NOT NULL,
    pgh_context_id uuid,
    to_court_id character varying(15) NOT NULL
);


ALTER TABLE public.search_courtappealstoevent OWNER TO django;

--
-- Name: search_courtappealstoevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.search_courtappealstoevent ALTER COLUMN pgh_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.search_courtappealstoevent_pgh_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: search_courtevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_courtevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id character varying(15) NOT NULL,
    pacer_court_id smallint,
    pacer_has_rss_feed boolean,
    pacer_rss_entry_types text NOT NULL,
    date_last_pacer_contact timestamp with time zone,
    fjc_court_id character varying(3) NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    in_use boolean NOT NULL,
    has_opinion_scraper boolean NOT NULL,
    has_oral_argument_scraper boolean NOT NULL,
    "position" double precision NOT NULL,
    citation_string character varying(100) NOT NULL,
    short_name character varying(100) NOT NULL,
    full_name character varying(200) NOT NULL,
    url character varying(500) NOT NULL,
    start_date date,
    end_date date,
    jurisdiction character varying(3) NOT NULL,
    notes text NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id character varying(15) NOT NULL,
    parent_court_id character varying(15),
    CONSTRAINT search_courtevent_pacer_court_id_check CHECK ((pacer_court_id >= 0))
);


ALTER TABLE public.search_courtevent OWNER TO django;

--
-- Name: search_courtevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_courtevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_courtevent_pgh_id_seq OWNER TO django;

--
-- Name: search_courtevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_courtevent_pgh_id_seq OWNED BY public.search_courtevent.pgh_id;


--
-- Name: search_courthouse; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_courthouse (
    id integer NOT NULL,
    court_seat boolean,
    building_name text NOT NULL,
    address1 text NOT NULL,
    address2 text NOT NULL,
    city text NOT NULL,
    county text NOT NULL,
    state character varying(2) NOT NULL,
    zip_code character varying(10) NOT NULL,
    country_code text NOT NULL,
    court_id character varying(15) NOT NULL
);


ALTER TABLE public.search_courthouse OWNER TO django;

--
-- Name: search_courthouse_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.search_courthouse ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.search_courthouse_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: search_courthouseevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_courthouseevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    court_seat boolean,
    building_name text NOT NULL,
    address1 text NOT NULL,
    address2 text NOT NULL,
    city text NOT NULL,
    county text NOT NULL,
    state character varying(2) NOT NULL,
    zip_code character varying(10) NOT NULL,
    country_code text NOT NULL,
    court_id character varying(15) NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_courthouseevent OWNER TO django;

--
-- Name: search_courthouseevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.search_courthouseevent ALTER COLUMN pgh_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.search_courthouseevent_pgh_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: search_docket_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docket_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docket_id_seq OWNER TO django;

--
-- Name: search_docket_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docket_id_seq OWNED BY public.search_docket.id;


--
-- Name: search_docket_panel; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docket_panel (
    id integer NOT NULL,
    docket_id integer NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.search_docket_panel OWNER TO django;

--
-- Name: search_docket_panel_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docket_panel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docket_panel_id_seq OWNER TO django;

--
-- Name: search_docket_panel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docket_panel_id_seq OWNED BY public.search_docket_panel.id;


--
-- Name: search_docket_tags; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docket_tags (
    id integer NOT NULL,
    docket_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_docket_tags OWNER TO django;

--
-- Name: search_docket_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docket_tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docket_tags_id_seq OWNER TO django;

--
-- Name: search_docket_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docket_tags_id_seq OWNED BY public.search_docket_tags.id;


--
-- Name: search_docketentry; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketentry (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_filed date,
    entry_number bigint,
    description text NOT NULL,
    docket_id integer NOT NULL,
    pacer_sequence_number integer,
    recap_sequence_number character varying(50) NOT NULL,
    time_filed time without time zone
);


ALTER TABLE public.search_docketentry OWNER TO django;

--
-- Name: search_docketentry_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketentry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketentry_id_seq OWNER TO django;

--
-- Name: search_docketentry_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketentry_id_seq OWNED BY public.search_docketentry.id;


--
-- Name: search_docketentry_tags; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketentry_tags (
    id integer NOT NULL,
    docketentry_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_docketentry_tags OWNER TO django;

--
-- Name: search_docketentry_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketentry_tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketentry_tags_id_seq OWNER TO django;

--
-- Name: search_docketentry_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketentry_tags_id_seq OWNED BY public.search_docketentry_tags.id;


--
-- Name: search_docketentryevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketentryevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_filed date,
    entry_number bigint,
    recap_sequence_number character varying(50) NOT NULL,
    pacer_sequence_number integer,
    description text NOT NULL,
    docket_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    time_filed time without time zone
);


ALTER TABLE public.search_docketentryevent OWNER TO django;

--
-- Name: search_docketentryevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketentryevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketentryevent_pgh_id_seq OWNER TO django;

--
-- Name: search_docketentryevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketentryevent_pgh_id_seq OWNED BY public.search_docketentryevent.pgh_id;


--
-- Name: search_docketentrytagsevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketentrytagsevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    docketentry_id integer NOT NULL,
    pgh_context_id uuid,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_docketentrytagsevent OWNER TO django;

--
-- Name: search_docketentrytagsevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketentrytagsevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketentrytagsevent_pgh_id_seq OWNER TO django;

--
-- Name: search_docketentrytagsevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketentrytagsevent_pgh_id_seq OWNED BY public.search_docketentrytagsevent.pgh_id;


--
-- Name: search_docketevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    source smallint NOT NULL,
    appeal_from_str text NOT NULL,
    assigned_to_str text NOT NULL,
    referred_to_str text NOT NULL,
    panel_str text NOT NULL,
    date_last_index timestamp with time zone,
    date_cert_granted date,
    date_cert_denied date,
    date_argued date,
    date_reargued date,
    date_reargument_denied date,
    date_filed date,
    date_terminated date,
    date_last_filing date,
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    slug character varying(75) NOT NULL,
    docket_number text,
    docket_number_core character varying(20) NOT NULL,
    pacer_case_id character varying(100),
    cause character varying(2000) NOT NULL,
    nature_of_suit character varying(1000) NOT NULL,
    jury_demand character varying(500) NOT NULL,
    jurisdiction_type character varying(100) NOT NULL,
    appellate_fee_status text NOT NULL,
    appellate_case_type_information text NOT NULL,
    mdl_status character varying(100) NOT NULL,
    filepath_local character varying(1000) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    filepath_ia_json character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    ia_needs_upload boolean,
    ia_date_first_change timestamp with time zone,
    date_blocked date,
    blocked boolean NOT NULL,
    appeal_from_id character varying(15),
    assigned_to_id integer,
    court_id character varying(15) NOT NULL,
    idb_data_id integer,
    originating_court_information_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    referred_to_id integer,
    federal_defendant_number smallint,
    federal_dn_case_type character varying(6) NOT NULL,
    federal_dn_judge_initials_assigned character varying(5) NOT NULL,
    federal_dn_judge_initials_referred character varying(5) NOT NULL,
    federal_dn_office_code character varying(3) NOT NULL,
    parent_docket_id integer
);


ALTER TABLE public.search_docketevent OWNER TO django;

--
-- Name: search_docketevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketevent_pgh_id_seq OWNER TO django;

--
-- Name: search_docketevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketevent_pgh_id_seq OWNED BY public.search_docketevent.pgh_id;


--
-- Name: search_docketpanelevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_docketpanelevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    docket_id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid
);


ALTER TABLE public.search_docketpanelevent OWNER TO django;

--
-- Name: search_docketpanelevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_docketpanelevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_docketpanelevent_pgh_id_seq OWNER TO django;

--
-- Name: search_docketpanelevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_docketpanelevent_pgh_id_seq OWNED BY public.search_docketpanelevent.pgh_id;


--
-- Name: search_dockettagsevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_dockettagsevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    docket_id integer NOT NULL,
    pgh_context_id uuid,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_dockettagsevent OWNER TO django;

--
-- Name: search_dockettagsevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_dockettagsevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_dockettagsevent_pgh_id_seq OWNER TO django;

--
-- Name: search_dockettagsevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_dockettagsevent_pgh_id_seq OWNED BY public.search_dockettagsevent.pgh_id;


--
-- Name: search_opinion_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinion_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinion_id_seq OWNER TO django;

--
-- Name: search_opinion_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinion_id_seq OWNED BY public.search_opinion.id;


--
-- Name: search_opinion_joined_by; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinion_joined_by (
    id integer NOT NULL,
    opinion_id integer NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.search_opinion_joined_by OWNER TO django;

--
-- Name: search_opinion_joined_by_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinion_joined_by_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinion_joined_by_id_seq OWNER TO django;

--
-- Name: search_opinion_joined_by_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinion_joined_by_id_seq OWNED BY public.search_opinion_joined_by.id;


--
-- Name: search_opinioncluster_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinioncluster_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinioncluster_id_seq OWNER TO django;

--
-- Name: search_opinioncluster_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinioncluster_id_seq OWNED BY public.search_opinioncluster.id;


--
-- Name: search_opinioncluster_non_participating_judges; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinioncluster_non_participating_judges (
    id integer NOT NULL,
    opinioncluster_id integer NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.search_opinioncluster_non_participating_judges OWNER TO django;

--
-- Name: search_opinioncluster_non_participating_judges_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinioncluster_non_participating_judges_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinioncluster_non_participating_judges_id_seq OWNER TO django;

--
-- Name: search_opinioncluster_non_participating_judges_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinioncluster_non_participating_judges_id_seq OWNED BY public.search_opinioncluster_non_participating_judges.id;


--
-- Name: search_opinioncluster_panel; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinioncluster_panel (
    id integer NOT NULL,
    opinioncluster_id integer NOT NULL,
    person_id integer NOT NULL
);


ALTER TABLE public.search_opinioncluster_panel OWNER TO django;

--
-- Name: search_opinioncluster_panel_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinioncluster_panel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinioncluster_panel_id_seq OWNER TO django;

--
-- Name: search_opinioncluster_panel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinioncluster_panel_id_seq OWNED BY public.search_opinioncluster_panel.id;


--
-- Name: search_opinionclusterevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionclusterevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    judges text NOT NULL,
    date_filed date NOT NULL,
    date_filed_is_approximate boolean NOT NULL,
    slug character varying(75),
    case_name_short text NOT NULL,
    case_name text NOT NULL,
    case_name_full text NOT NULL,
    scdb_id character varying(10) NOT NULL,
    scdb_decision_direction integer,
    scdb_votes_majority integer,
    scdb_votes_minority integer,
    source character varying(10) NOT NULL,
    procedural_history text NOT NULL,
    attorneys text NOT NULL,
    nature_of_suit text NOT NULL,
    posture text NOT NULL,
    syllabus text NOT NULL,
    headnotes text NOT NULL,
    summary text NOT NULL,
    disposition text NOT NULL,
    history text NOT NULL,
    other_dates text NOT NULL,
    cross_reference text NOT NULL,
    correction text NOT NULL,
    citation_count integer NOT NULL,
    precedential_status character varying(50) NOT NULL,
    date_blocked date,
    blocked boolean NOT NULL,
    filepath_json_harvard character varying(1000) NOT NULL,
    docket_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    arguments text NOT NULL,
    headmatter text NOT NULL,
    filepath_pdf_harvard character varying(100) NOT NULL
);


ALTER TABLE public.search_opinionclusterevent OWNER TO django;

--
-- Name: search_opinionclusterevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionclusterevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionclusterevent_pgh_id_seq OWNER TO django;

--
-- Name: search_opinionclusterevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionclusterevent_pgh_id_seq OWNED BY public.search_opinionclusterevent.pgh_id;


--
-- Name: search_opinionclusternonparticipatingjudgesevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionclusternonparticipatingjudgesevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    opinioncluster_id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid
);


ALTER TABLE public.search_opinionclusternonparticipatingjudgesevent OWNER TO django;

--
-- Name: search_opinionclusternonparticipatingjudgesevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionclusternonparticipatingjudgesevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionclusternonparticipatingjudgesevent_pgh_id_seq OWNER TO django;

--
-- Name: search_opinionclusternonparticipatingjudgesevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionclusternonparticipatingjudgesevent_pgh_id_seq OWNED BY public.search_opinionclusternonparticipatingjudgesevent.pgh_id;


--
-- Name: search_opinionclusterpanelevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionclusterpanelevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    opinioncluster_id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid
);


ALTER TABLE public.search_opinionclusterpanelevent OWNER TO django;

--
-- Name: search_opinionclusterpanelevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionclusterpanelevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionclusterpanelevent_pgh_id_seq OWNER TO django;

--
-- Name: search_opinionclusterpanelevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionclusterpanelevent_pgh_id_seq OWNED BY public.search_opinionclusterpanelevent.pgh_id;


--
-- Name: search_opinionevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    author_str text NOT NULL,
    per_curiam boolean NOT NULL,
    joined_by_str text NOT NULL,
    type character varying(20) NOT NULL,
    sha1 character varying(40) NOT NULL,
    page_count integer,
    download_url character varying(500),
    local_path character varying(100) NOT NULL,
    plain_text text NOT NULL,
    html text NOT NULL,
    html_lawbox text NOT NULL,
    html_columbia text NOT NULL,
    html_anon_2020 text NOT NULL,
    xml_harvard text NOT NULL,
    html_with_citations text NOT NULL,
    extracted_by_ocr boolean NOT NULL,
    author_id integer,
    cluster_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    ordering_key integer
);


ALTER TABLE public.search_opinionevent OWNER TO django;

--
-- Name: search_opinionevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionevent_pgh_id_seq OWNER TO django;

--
-- Name: search_opinionevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionevent_pgh_id_seq OWNED BY public.search_opinionevent.pgh_id;


--
-- Name: search_opinionjoinedbyevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionjoinedbyevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    opinion_id integer NOT NULL,
    person_id integer NOT NULL,
    pgh_context_id uuid
);


ALTER TABLE public.search_opinionjoinedbyevent OWNER TO django;

--
-- Name: search_opinionjoinedbyevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionjoinedbyevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionjoinedbyevent_pgh_id_seq OWNER TO django;

--
-- Name: search_opinionjoinedbyevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionjoinedbyevent_pgh_id_seq OWNED BY public.search_opinionjoinedbyevent.pgh_id;


--
-- Name: search_opinionscited; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionscited (
    id integer NOT NULL,
    cited_opinion_id integer NOT NULL,
    citing_opinion_id integer NOT NULL,
    depth integer NOT NULL
);


ALTER TABLE public.search_opinionscited OWNER TO django;

--
-- Name: search_opinionscited_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionscited_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionscited_id_seq OWNER TO django;

--
-- Name: search_opinionscited_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionscited_id_seq OWNED BY public.search_opinionscited.id;


--
-- Name: search_opinionscitedbyrecapdocument; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_opinionscitedbyrecapdocument (
    id integer NOT NULL,
    depth integer NOT NULL,
    cited_opinion_id integer NOT NULL,
    citing_document_id integer NOT NULL
);


ALTER TABLE public.search_opinionscitedbyrecapdocument OWNER TO django;

--
-- Name: search_opinionscitedbyrecapdocument_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_opinionscitedbyrecapdocument_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_opinionscitedbyrecapdocument_id_seq OWNER TO django;

--
-- Name: search_opinionscitedbyrecapdocument_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_opinionscitedbyrecapdocument_id_seq OWNED BY public.search_opinionscitedbyrecapdocument.id;


--
-- Name: search_originatingcourtinformation; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_originatingcourtinformation (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    assigned_to_str text NOT NULL,
    court_reporter text NOT NULL,
    date_disposed date,
    date_filed date,
    date_judgment date,
    date_judgment_eod date,
    date_filed_noa date,
    date_received_coa date,
    assigned_to_id integer,
    docket_number text NOT NULL,
    ordering_judge_id integer,
    ordering_judge_str text NOT NULL
);


ALTER TABLE public.search_originatingcourtinformation OWNER TO django;

--
-- Name: search_originatingcourtinformation_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_originatingcourtinformation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_originatingcourtinformation_id_seq OWNER TO django;

--
-- Name: search_originatingcourtinformation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_originatingcourtinformation_id_seq OWNED BY public.search_originatingcourtinformation.id;


--
-- Name: search_originatingcourtinformationevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_originatingcourtinformationevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    docket_number text NOT NULL,
    assigned_to_str text NOT NULL,
    ordering_judge_str text NOT NULL,
    court_reporter text NOT NULL,
    date_disposed date,
    date_filed date,
    date_judgment date,
    date_judgment_eod date,
    date_filed_noa date,
    date_received_coa date,
    assigned_to_id integer,
    ordering_judge_id integer,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_originatingcourtinformationevent OWNER TO django;

--
-- Name: search_originatingcourtinformationevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_originatingcourtinformationevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_originatingcourtinformationevent_pgh_id_seq OWNER TO django;

--
-- Name: search_originatingcourtinformationevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_originatingcourtinformationevent_pgh_id_seq OWNED BY public.search_originatingcourtinformationevent.pgh_id;


--
-- Name: search_parenthetical; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_parenthetical (
    id integer NOT NULL,
    text text NOT NULL,
    score double precision NOT NULL,
    described_opinion_id integer NOT NULL,
    describing_opinion_id integer NOT NULL,
    group_id integer
);


ALTER TABLE public.search_parenthetical OWNER TO django;

--
-- Name: search_parenthetical_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_parenthetical_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_parenthetical_id_seq OWNER TO django;

--
-- Name: search_parenthetical_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_parenthetical_id_seq OWNED BY public.search_parenthetical.id;


--
-- Name: search_parentheticalgroup; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_parentheticalgroup (
    id integer NOT NULL,
    score double precision NOT NULL,
    size integer NOT NULL,
    opinion_id integer NOT NULL,
    representative_id integer NOT NULL
);


ALTER TABLE public.search_parentheticalgroup OWNER TO django;

--
-- Name: search_parentheticalgroup_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_parentheticalgroup_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_parentheticalgroup_id_seq OWNER TO django;

--
-- Name: search_parentheticalgroup_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_parentheticalgroup_id_seq OWNED BY public.search_parentheticalgroup.id;


--
-- Name: search_recapdocument; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_recapdocument (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    date_upload timestamp with time zone,
    document_type integer NOT NULL,
    document_number character varying(32) NOT NULL,
    attachment_number smallint,
    pacer_doc_id character varying(64) NOT NULL,
    is_available boolean,
    sha1 character varying(40) NOT NULL,
    filepath_local character varying(1000) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    docket_entry_id integer NOT NULL,
    description text NOT NULL,
    ocr_status smallint,
    plain_text text NOT NULL,
    page_count integer,
    is_free_on_pacer boolean,
    ia_upload_failure_count smallint,
    file_size integer,
    thumbnail character varying(100),
    thumbnail_status smallint NOT NULL,
    is_sealed boolean,
    acms_document_guid character varying(64) NOT NULL
);


ALTER TABLE public.search_recapdocument OWNER TO django;

--
-- Name: search_recapdocument_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_recapdocument_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_recapdocument_id_seq OWNER TO django;

--
-- Name: search_recapdocument_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_recapdocument_id_seq OWNED BY public.search_recapdocument.id;


--
-- Name: search_recapdocument_tags; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_recapdocument_tags (
    id integer NOT NULL,
    recapdocument_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_recapdocument_tags OWNER TO django;

--
-- Name: search_recapdocument_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_recapdocument_tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_recapdocument_tags_id_seq OWNER TO django;

--
-- Name: search_recapdocument_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_recapdocument_tags_id_seq OWNED BY public.search_recapdocument_tags.id;


--
-- Name: search_recapdocumentevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_recapdocumentevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    sha1 character varying(40) NOT NULL,
    page_count integer,
    file_size integer,
    filepath_local character varying(1000) NOT NULL,
    filepath_ia character varying(1000) NOT NULL,
    ia_upload_failure_count smallint,
    thumbnail character varying(100),
    thumbnail_status smallint NOT NULL,
    plain_text text NOT NULL,
    ocr_status smallint,
    date_upload timestamp with time zone,
    document_number character varying(32) NOT NULL,
    attachment_number smallint,
    pacer_doc_id character varying(64) NOT NULL,
    is_available boolean,
    is_free_on_pacer boolean,
    is_sealed boolean,
    document_type integer NOT NULL,
    description text NOT NULL,
    docket_entry_id integer NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL,
    acms_document_guid character varying(64) NOT NULL
);


ALTER TABLE public.search_recapdocumentevent OWNER TO django;

--
-- Name: search_recapdocumentevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_recapdocumentevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_recapdocumentevent_pgh_id_seq OWNER TO django;

--
-- Name: search_recapdocumentevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_recapdocumentevent_pgh_id_seq OWNED BY public.search_recapdocumentevent.pgh_id;


--
-- Name: search_recapdocumenttagsevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_recapdocumenttagsevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    pgh_context_id uuid,
    recapdocument_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE public.search_recapdocumenttagsevent OWNER TO django;

--
-- Name: search_recapdocumenttagsevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_recapdocumenttagsevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_recapdocumenttagsevent_pgh_id_seq OWNER TO django;

--
-- Name: search_recapdocumenttagsevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_recapdocumenttagsevent_pgh_id_seq OWNED BY public.search_recapdocumenttagsevent.pgh_id;


--
-- Name: search_searchquery; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_searchquery (
    id integer NOT NULL,
    source smallint NOT NULL,
    get_params text NOT NULL,
    query_time_ms integer,
    hit_cache boolean NOT NULL,
    failed boolean NOT NULL,
    engine smallint NOT NULL,
    date_created timestamp with time zone NOT NULL,
    user_id integer
);


ALTER TABLE public.search_searchquery OWNER TO django;

--
-- Name: search_searchquery_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

ALTER TABLE public.search_searchquery ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.search_searchquery_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: search_tag; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_tag (
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name character varying(50) NOT NULL
);


ALTER TABLE public.search_tag OWNER TO django;

--
-- Name: search_tag_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_tag_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_tag_id_seq OWNER TO django;

--
-- Name: search_tag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_tag_id_seq OWNED BY public.search_tag.id;


--
-- Name: search_tagevent; Type: TABLE; Schema: public; Owner: django
--

CREATE TABLE public.search_tagevent (
    pgh_id integer NOT NULL,
    pgh_created_at timestamp with time zone NOT NULL,
    pgh_label text NOT NULL,
    id integer NOT NULL,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL,
    name character varying(50) NOT NULL,
    pgh_context_id uuid,
    pgh_obj_id integer NOT NULL
);


ALTER TABLE public.search_tagevent OWNER TO django;

--
-- Name: search_tagevent_pgh_id_seq; Type: SEQUENCE; Schema: public; Owner: django
--

CREATE SEQUENCE public.search_tagevent_pgh_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.search_tagevent_pgh_id_seq OWNER TO django;

--
-- Name: search_tagevent_pgh_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: django
--

ALTER SEQUENCE public.search_tagevent_pgh_id_seq OWNED BY public.search_tagevent.pgh_id;


--
-- Name: audio_audio id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio ALTER COLUMN id SET DEFAULT nextval('public.audio_audio_id_seq'::regclass);


--
-- Name: audio_audio_panel id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio_panel ALTER COLUMN id SET DEFAULT nextval('public.audio_audio_panel_id_seq'::regclass);


--
-- Name: audio_audioevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audioevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.audio_audioevent_pgh_id_seq'::regclass);


--
-- Name: audio_audiopanelevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audiopanelevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.audio_audiopanelevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_agreement id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_agreement ALTER COLUMN id SET DEFAULT nextval('public.disclosures_agreement_id_seq'::regclass);


--
-- Name: disclosures_agreementevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_agreementevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_agreementevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_debt id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_debt ALTER COLUMN id SET DEFAULT nextval('public.disclosures_debt_id_seq'::regclass);


--
-- Name: disclosures_debtevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_debtevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_debtevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_financialdisclosure id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosure ALTER COLUMN id SET DEFAULT nextval('public.disclosures_financialdisclosure_id_seq'::regclass);


--
-- Name: disclosures_financialdisclosureevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosureevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_financialdisclosureevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_gift id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_gift ALTER COLUMN id SET DEFAULT nextval('public.disclosures_gift_id_seq'::regclass);


--
-- Name: disclosures_giftevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_giftevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_giftevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_investment id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_investment ALTER COLUMN id SET DEFAULT nextval('public.disclosures_investment_id_seq'::regclass);


--
-- Name: disclosures_investmentevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_investmentevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_investmentevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_noninvestmentincome id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_noninvestmentincome ALTER COLUMN id SET DEFAULT nextval('public.disclosures_noninvestmentincome_id_seq'::regclass);


--
-- Name: disclosures_noninvestmentincomeevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_noninvestmentincomeevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_noninvestmentincomeevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_position id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_position ALTER COLUMN id SET DEFAULT nextval('public.disclosures_position_id_seq'::regclass);


--
-- Name: disclosures_positionevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_positionevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_positionevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_reimbursement id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_reimbursement ALTER COLUMN id SET DEFAULT nextval('public.disclosures_reimbursement_id_seq'::regclass);


--
-- Name: disclosures_reimbursementevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_reimbursementevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_reimbursementevent_pgh_id_seq'::regclass);


--
-- Name: disclosures_spouseincome id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_spouseincome ALTER COLUMN id SET DEFAULT nextval('public.disclosures_spouseincome_id_seq'::regclass);


--
-- Name: disclosures_spouseincomeevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_spouseincomeevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.disclosures_spouseincomeevent_pgh_id_seq'::regclass);


--
-- Name: people_db_abarating id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_abarating ALTER COLUMN id SET DEFAULT nextval('public.people_db_abarating_id_seq'::regclass);


--
-- Name: people_db_abaratingevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_abaratingevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_abaratingevent_pgh_id_seq'::regclass);


--
-- Name: people_db_attorney id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorney ALTER COLUMN id SET DEFAULT nextval('public.people_db_attorney_id_seq'::regclass);


--
-- Name: people_db_attorneyorganization id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganization ALTER COLUMN id SET DEFAULT nextval('public.people_db_attorneyorganization_id_seq'::regclass);


--
-- Name: people_db_attorneyorganizationassociation id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation ALTER COLUMN id SET DEFAULT nextval('public.people_db_attorneyorganizationassociation_id_seq'::regclass);


--
-- Name: people_db_criminalcomplaint id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcomplaint ALTER COLUMN id SET DEFAULT nextval('public.people_db_criminalcomplaint_id_seq'::regclass);


--
-- Name: people_db_criminalcount id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcount ALTER COLUMN id SET DEFAULT nextval('public.people_db_criminalcount_id_seq'::regclass);


--
-- Name: people_db_education id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_education ALTER COLUMN id SET DEFAULT nextval('public.people_db_education_id_seq'::regclass);


--
-- Name: people_db_educationevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_educationevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_educationevent_pgh_id_seq'::regclass);


--
-- Name: people_db_party id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_party ALTER COLUMN id SET DEFAULT nextval('public.people_db_party_id_seq'::regclass);


--
-- Name: people_db_partytype id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_partytype ALTER COLUMN id SET DEFAULT nextval('public.people_db_partytype_id_seq'::regclass);


--
-- Name: people_db_person id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person ALTER COLUMN id SET DEFAULT nextval('public.people_db_person_id_seq'::regclass);


--
-- Name: people_db_person_race id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person_race ALTER COLUMN id SET DEFAULT nextval('public.people_db_person_race_id_seq'::regclass);


--
-- Name: people_db_personevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_personevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_personevent_pgh_id_seq'::regclass);


--
-- Name: people_db_personraceevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_personraceevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_personraceevent_pgh_id_seq'::regclass);


--
-- Name: people_db_politicalaffiliation id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_politicalaffiliation ALTER COLUMN id SET DEFAULT nextval('public.people_db_politicalaffiliation_id_seq'::regclass);


--
-- Name: people_db_politicalaffiliationevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_politicalaffiliationevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_politicalaffiliationevent_pgh_id_seq'::regclass);


--
-- Name: people_db_position id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position ALTER COLUMN id SET DEFAULT nextval('public.people_db_position_id_seq'::regclass);


--
-- Name: people_db_positionevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_positionevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_positionevent_pgh_id_seq'::regclass);


--
-- Name: people_db_race id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_race ALTER COLUMN id SET DEFAULT nextval('public.people_db_race_id_seq'::regclass);


--
-- Name: people_db_raceevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_raceevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_raceevent_pgh_id_seq'::regclass);


--
-- Name: people_db_retentionevent id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_retentionevent ALTER COLUMN id SET DEFAULT nextval('public.people_db_retentionevent_id_seq'::regclass);


--
-- Name: people_db_retentioneventevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_retentioneventevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_retentioneventevent_pgh_id_seq'::regclass);


--
-- Name: people_db_role id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role ALTER COLUMN id SET DEFAULT nextval('public.people_db_role_id_seq'::regclass);


--
-- Name: people_db_school id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_school ALTER COLUMN id SET DEFAULT nextval('public.people_db_school_id_seq'::regclass);


--
-- Name: people_db_schoolevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_schoolevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_schoolevent_pgh_id_seq'::regclass);


--
-- Name: people_db_source id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_source ALTER COLUMN id SET DEFAULT nextval('public.people_db_source_id_seq'::regclass);


--
-- Name: people_db_sourceevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_sourceevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.people_db_sourceevent_pgh_id_seq'::regclass);


--
-- Name: recap_emailprocessingqueue id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue ALTER COLUMN id SET DEFAULT nextval('public.recap_emailprocessingqueue_id_seq'::regclass);


--
-- Name: recap_emailprocessingqueue_recap_documents id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue_recap_documents ALTER COLUMN id SET DEFAULT nextval('public.recap_emailprocessingqueue_recap_documents_id_seq'::regclass);


--
-- Name: recap_fjcintegrateddatabase id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_fjcintegrateddatabase ALTER COLUMN id SET DEFAULT nextval('public.recap_fjcintegrateddatabase_id_seq'::regclass);


--
-- Name: recap_pacerfetchqueue id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue ALTER COLUMN id SET DEFAULT nextval('public.recap_pacerfetchqueue_id_seq'::regclass);


--
-- Name: recap_pacerhtmlfiles id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerhtmlfiles ALTER COLUMN id SET DEFAULT nextval('public.recap_pacerhtmlfiles_id_seq'::regclass);


--
-- Name: recap_processingqueue id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue ALTER COLUMN id SET DEFAULT nextval('public.recap_processingqueue_id_seq'::regclass);


--
-- Name: recap_rss_rssfeeddata id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeeddata ALTER COLUMN id SET DEFAULT nextval('public.recap_rss_rssfeeddata_id_seq'::regclass);


--
-- Name: recap_rss_rssfeedstatus id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeedstatus ALTER COLUMN id SET DEFAULT nextval('public.recap_rss_rssfeedstatus_id_seq'::regclass);


--
-- Name: recap_rss_rssitemcache id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssitemcache ALTER COLUMN id SET DEFAULT nextval('public.recap_rss_rssitemcache_id_seq'::regclass);


--
-- Name: search_bankruptcyinformation id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformation ALTER COLUMN id SET DEFAULT nextval('public.search_bankruptcyinformation_id_seq'::regclass);


--
-- Name: search_bankruptcyinformationevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformationevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_bankruptcyinformationevent_pgh_id_seq'::regclass);


--
-- Name: search_citation id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citation ALTER COLUMN id SET DEFAULT nextval('public.search_citation_id_seq'::regclass);


--
-- Name: search_citationevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citationevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_citationevent_pgh_id_seq'::regclass);


--
-- Name: search_claim id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim ALTER COLUMN id SET DEFAULT nextval('public.search_claim_id_seq'::regclass);


--
-- Name: search_claim_tags id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim_tags ALTER COLUMN id SET DEFAULT nextval('public.search_claim_tags_id_seq'::regclass);


--
-- Name: search_claimevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_claimevent_pgh_id_seq'::regclass);


--
-- Name: search_claimhistory id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimhistory ALTER COLUMN id SET DEFAULT nextval('public.search_claimhistory_id_seq'::regclass);


--
-- Name: search_claimhistoryevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimhistoryevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_claimhistoryevent_pgh_id_seq'::regclass);


--
-- Name: search_claimtagsevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimtagsevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_claimtagsevent_pgh_id_seq'::regclass);


--
-- Name: search_courtevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courtevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_courtevent_pgh_id_seq'::regclass);


--
-- Name: search_docket id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket ALTER COLUMN id SET DEFAULT nextval('public.search_docket_id_seq'::regclass);


--
-- Name: search_docket_panel id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_panel ALTER COLUMN id SET DEFAULT nextval('public.search_docket_panel_id_seq'::regclass);


--
-- Name: search_docket_tags id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_tags ALTER COLUMN id SET DEFAULT nextval('public.search_docket_tags_id_seq'::regclass);


--
-- Name: search_docketentry id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry ALTER COLUMN id SET DEFAULT nextval('public.search_docketentry_id_seq'::regclass);


--
-- Name: search_docketentry_tags id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry_tags ALTER COLUMN id SET DEFAULT nextval('public.search_docketentry_tags_id_seq'::regclass);


--
-- Name: search_docketentryevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentryevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_docketentryevent_pgh_id_seq'::regclass);


--
-- Name: search_docketentrytagsevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentrytagsevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_docketentrytagsevent_pgh_id_seq'::regclass);


--
-- Name: search_docketevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_docketevent_pgh_id_seq'::regclass);


--
-- Name: search_docketpanelevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketpanelevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_docketpanelevent_pgh_id_seq'::regclass);


--
-- Name: search_dockettagsevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_dockettagsevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_dockettagsevent_pgh_id_seq'::regclass);


--
-- Name: search_opinion id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion ALTER COLUMN id SET DEFAULT nextval('public.search_opinion_id_seq'::regclass);


--
-- Name: search_opinion_joined_by id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion_joined_by ALTER COLUMN id SET DEFAULT nextval('public.search_opinion_joined_by_id_seq'::regclass);


--
-- Name: search_opinioncluster id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster ALTER COLUMN id SET DEFAULT nextval('public.search_opinioncluster_id_seq'::regclass);


--
-- Name: search_opinioncluster_non_participating_judges id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_non_participating_judges ALTER COLUMN id SET DEFAULT nextval('public.search_opinioncluster_non_participating_judges_id_seq'::regclass);


--
-- Name: search_opinioncluster_panel id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_panel ALTER COLUMN id SET DEFAULT nextval('public.search_opinioncluster_panel_id_seq'::regclass);


--
-- Name: search_opinionclusterevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusterevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_opinionclusterevent_pgh_id_seq'::regclass);


--
-- Name: search_opinionclusternonparticipatingjudgesevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusternonparticipatingjudgesevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_opinionclusternonparticipatingjudgesevent_pgh_id_seq'::regclass);


--
-- Name: search_opinionclusterpanelevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusterpanelevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_opinionclusterpanelevent_pgh_id_seq'::regclass);


--
-- Name: search_opinionevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_opinionevent_pgh_id_seq'::regclass);


--
-- Name: search_opinionjoinedbyevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionjoinedbyevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_opinionjoinedbyevent_pgh_id_seq'::regclass);


--
-- Name: search_opinionscited id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscited ALTER COLUMN id SET DEFAULT nextval('public.search_opinionscited_id_seq'::regclass);


--
-- Name: search_opinionscitedbyrecapdocument id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscitedbyrecapdocument ALTER COLUMN id SET DEFAULT nextval('public.search_opinionscitedbyrecapdocument_id_seq'::regclass);


--
-- Name: search_originatingcourtinformation id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformation ALTER COLUMN id SET DEFAULT nextval('public.search_originatingcourtinformation_id_seq'::regclass);


--
-- Name: search_originatingcourtinformationevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformationevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_originatingcourtinformationevent_pgh_id_seq'::regclass);


--
-- Name: search_parenthetical id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parenthetical ALTER COLUMN id SET DEFAULT nextval('public.search_parenthetical_id_seq'::regclass);


--
-- Name: search_parentheticalgroup id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parentheticalgroup ALTER COLUMN id SET DEFAULT nextval('public.search_parentheticalgroup_id_seq'::regclass);


--
-- Name: search_recapdocument id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument ALTER COLUMN id SET DEFAULT nextval('public.search_recapdocument_id_seq'::regclass);


--
-- Name: search_recapdocument_tags id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument_tags ALTER COLUMN id SET DEFAULT nextval('public.search_recapdocument_tags_id_seq'::regclass);


--
-- Name: search_recapdocumentevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocumentevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_recapdocumentevent_pgh_id_seq'::regclass);


--
-- Name: search_recapdocumenttagsevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocumenttagsevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_recapdocumenttagsevent_pgh_id_seq'::regclass);


--
-- Name: search_tag id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_tag ALTER COLUMN id SET DEFAULT nextval('public.search_tag_id_seq'::regclass);


--
-- Name: search_tagevent pgh_id; Type: DEFAULT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_tagevent ALTER COLUMN pgh_id SET DEFAULT nextval('public.search_tagevent_pgh_id_seq'::regclass);


--
-- Name: audio_audio_panel audio_audio_panel_audio_id_judge_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio_panel
    ADD CONSTRAINT audio_audio_panel_audio_id_judge_id_key UNIQUE (audio_id, person_id);


--
-- Name: audio_audio_panel audio_audio_panel_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio_panel
    ADD CONSTRAINT audio_audio_panel_pkey PRIMARY KEY (id);


--
-- Name: audio_audio audio_audio_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio
    ADD CONSTRAINT audio_audio_pkey PRIMARY KEY (id);


--
-- Name: audio_audioevent audio_audioevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audioevent
    ADD CONSTRAINT audio_audioevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: audio_audiopanelevent audio_audiopanelevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audiopanelevent
    ADD CONSTRAINT audio_audiopanelevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: audio_audiotranscriptionmetadata audio_audiotranscriptionmetadata_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audiotranscriptionmetadata
    ADD CONSTRAINT audio_audiotranscriptionmetadata_pkey PRIMARY KEY (id);


--
-- Name: disclosures_agreement disclosures_agreement_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_agreement
    ADD CONSTRAINT disclosures_agreement_pkey PRIMARY KEY (id);


--
-- Name: disclosures_agreementevent disclosures_agreementevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_agreementevent
    ADD CONSTRAINT disclosures_agreementevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_debt disclosures_debt_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_debt
    ADD CONSTRAINT disclosures_debt_pkey PRIMARY KEY (id);


--
-- Name: disclosures_debtevent disclosures_debtevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_debtevent
    ADD CONSTRAINT disclosures_debtevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_financialdisclosure disclosures_financialdisclosure_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosure
    ADD CONSTRAINT disclosures_financialdisclosure_pkey PRIMARY KEY (id);


--
-- Name: disclosures_financialdisclosure disclosures_financialdisclosure_sha1_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosure
    ADD CONSTRAINT disclosures_financialdisclosure_sha1_key UNIQUE (sha1);


--
-- Name: disclosures_financialdisclosureevent disclosures_financialdisclosureevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosureevent
    ADD CONSTRAINT disclosures_financialdisclosureevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_gift disclosures_gift_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_gift
    ADD CONSTRAINT disclosures_gift_pkey PRIMARY KEY (id);


--
-- Name: disclosures_giftevent disclosures_giftevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_giftevent
    ADD CONSTRAINT disclosures_giftevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_investment disclosures_investment_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_investment
    ADD CONSTRAINT disclosures_investment_pkey PRIMARY KEY (id);


--
-- Name: disclosures_investmentevent disclosures_investmentevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_investmentevent
    ADD CONSTRAINT disclosures_investmentevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_noninvestmentincome disclosures_noninvestmentincome_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_noninvestmentincome
    ADD CONSTRAINT disclosures_noninvestmentincome_pkey PRIMARY KEY (id);


--
-- Name: disclosures_noninvestmentincomeevent disclosures_noninvestmentincomeevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_noninvestmentincomeevent
    ADD CONSTRAINT disclosures_noninvestmentincomeevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_position disclosures_position_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_position
    ADD CONSTRAINT disclosures_position_pkey PRIMARY KEY (id);


--
-- Name: disclosures_positionevent disclosures_positionevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_positionevent
    ADD CONSTRAINT disclosures_positionevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_reimbursement disclosures_reimbursement_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_reimbursement
    ADD CONSTRAINT disclosures_reimbursement_pkey PRIMARY KEY (id);


--
-- Name: disclosures_reimbursementevent disclosures_reimbursementevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_reimbursementevent
    ADD CONSTRAINT disclosures_reimbursementevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: disclosures_spouseincome disclosures_spouseincome_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_spouseincome
    ADD CONSTRAINT disclosures_spouseincome_pkey PRIMARY KEY (id);


--
-- Name: disclosures_spouseincomeevent disclosures_spouseincomeevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_spouseincomeevent
    ADD CONSTRAINT disclosures_spouseincomeevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_abarating people_db_abarating_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_abarating
    ADD CONSTRAINT people_db_abarating_pkey PRIMARY KEY (id);


--
-- Name: people_db_abaratingevent people_db_abaratingevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_abaratingevent
    ADD CONSTRAINT people_db_abaratingevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_attorney people_db_attorney_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorney
    ADD CONSTRAINT people_db_attorney_pkey PRIMARY KEY (id);


--
-- Name: people_db_attorneyorganizationassociation people_db_attorneyorganization_attorney_id_7cda1fb15b747f5_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation
    ADD CONSTRAINT people_db_attorneyorganization_attorney_id_7cda1fb15b747f5_uniq UNIQUE (attorney_id, attorney_organization_id, docket_id);


--
-- Name: people_db_attorneyorganization people_db_attorneyorganization_lookup_key_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganization
    ADD CONSTRAINT people_db_attorneyorganization_lookup_key_key UNIQUE (lookup_key);


--
-- Name: people_db_attorneyorganization people_db_attorneyorganization_name_6e4a7d6ba93cb6a6_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganization
    ADD CONSTRAINT people_db_attorneyorganization_name_6e4a7d6ba93cb6a6_uniq UNIQUE (name, address1, address2, city, state, zip_code);


--
-- Name: people_db_attorneyorganization people_db_attorneyorganization_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganization
    ADD CONSTRAINT people_db_attorneyorganization_pkey PRIMARY KEY (id);


--
-- Name: people_db_attorneyorganizationassociation people_db_attorneyorganizationassociation_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation
    ADD CONSTRAINT people_db_attorneyorganizationassociation_pkey PRIMARY KEY (id);


--
-- Name: people_db_criminalcomplaint people_db_criminalcomplaint_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcomplaint
    ADD CONSTRAINT people_db_criminalcomplaint_pkey PRIMARY KEY (id);


--
-- Name: people_db_criminalcount people_db_criminalcount_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcount
    ADD CONSTRAINT people_db_criminalcount_pkey PRIMARY KEY (id);


--
-- Name: people_db_education people_db_education_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_education
    ADD CONSTRAINT people_db_education_pkey PRIMARY KEY (id);


--
-- Name: people_db_educationevent people_db_educationevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_educationevent
    ADD CONSTRAINT people_db_educationevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_party people_db_party_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_party
    ADD CONSTRAINT people_db_party_pkey PRIMARY KEY (id);


--
-- Name: people_db_partytype people_db_partytype_docket_id_345b40b72c694865_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_partytype
    ADD CONSTRAINT people_db_partytype_docket_id_345b40b72c694865_uniq UNIQUE (docket_id, party_id, name);


--
-- Name: people_db_partytype people_db_partytype_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_partytype
    ADD CONSTRAINT people_db_partytype_pkey PRIMARY KEY (id);


--
-- Name: people_db_person people_db_person_fjc_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person
    ADD CONSTRAINT people_db_person_fjc_id_key UNIQUE (fjc_id);


--
-- Name: people_db_person people_db_person_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person
    ADD CONSTRAINT people_db_person_pkey PRIMARY KEY (id);


--
-- Name: people_db_person_race people_db_person_race_person_id_race_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person_race
    ADD CONSTRAINT people_db_person_race_person_id_race_id_key UNIQUE (person_id, race_id);


--
-- Name: people_db_person_race people_db_person_race_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person_race
    ADD CONSTRAINT people_db_person_race_pkey PRIMARY KEY (id);


--
-- Name: people_db_personevent people_db_personevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_personevent
    ADD CONSTRAINT people_db_personevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_personraceevent people_db_personraceevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_personraceevent
    ADD CONSTRAINT people_db_personraceevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_politicalaffiliation people_db_politicalaffiliation_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_politicalaffiliation
    ADD CONSTRAINT people_db_politicalaffiliation_pkey PRIMARY KEY (id);


--
-- Name: people_db_politicalaffiliationevent people_db_politicalaffiliationevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_politicalaffiliationevent
    ADD CONSTRAINT people_db_politicalaffiliationevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_position people_db_position_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_db_position_pkey PRIMARY KEY (id);


--
-- Name: people_db_positionevent people_db_positionevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_positionevent
    ADD CONSTRAINT people_db_positionevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_race people_db_race_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_race
    ADD CONSTRAINT people_db_race_pkey PRIMARY KEY (id);


--
-- Name: people_db_race people_db_race_race_50897822747d246e_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_race
    ADD CONSTRAINT people_db_race_race_50897822747d246e_uniq UNIQUE (race);


--
-- Name: people_db_raceevent people_db_raceevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_raceevent
    ADD CONSTRAINT people_db_raceevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_retentionevent people_db_retentionevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_retentionevent
    ADD CONSTRAINT people_db_retentionevent_pkey PRIMARY KEY (id);


--
-- Name: people_db_retentioneventevent people_db_retentioneventevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_retentioneventevent
    ADD CONSTRAINT people_db_retentioneventevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_role people_db_role_party_id_73709a165082400a_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role
    ADD CONSTRAINT people_db_role_party_id_73709a165082400a_uniq UNIQUE (party_id, attorney_id, role, docket_id, date_action);


--
-- Name: people_db_role people_db_role_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role
    ADD CONSTRAINT people_db_role_pkey PRIMARY KEY (id);


--
-- Name: people_db_school people_db_school_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_school
    ADD CONSTRAINT people_db_school_pkey PRIMARY KEY (id);


--
-- Name: people_db_schoolevent people_db_schoolevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_schoolevent
    ADD CONSTRAINT people_db_schoolevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: people_db_source people_db_source_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_source
    ADD CONSTRAINT people_db_source_pkey PRIMARY KEY (id);


--
-- Name: people_db_sourceevent people_db_sourceevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_sourceevent
    ADD CONSTRAINT people_db_sourceevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: recap_emailprocessingqueue_recap_documents recap_emailprocessingque_emailprocessingqueue_id__25151718_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue_recap_documents
    ADD CONSTRAINT recap_emailprocessingque_emailprocessingqueue_id__25151718_uniq UNIQUE (emailprocessingqueue_id, recapdocument_id);


--
-- Name: recap_emailprocessingqueue recap_emailprocessingqueue_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue
    ADD CONSTRAINT recap_emailprocessingqueue_pkey PRIMARY KEY (id);


--
-- Name: recap_emailprocessingqueue_recap_documents recap_emailprocessingqueue_recap_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue_recap_documents
    ADD CONSTRAINT recap_emailprocessingqueue_recap_documents_pkey PRIMARY KEY (id);


--
-- Name: recap_fjcintegrateddatabase recap_fjcintegrateddatabase_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_fjcintegrateddatabase
    ADD CONSTRAINT recap_fjcintegrateddatabase_pkey PRIMARY KEY (id);


--
-- Name: recap_pacerfetchqueue recap_pacerfetchqueue_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue
    ADD CONSTRAINT recap_pacerfetchqueue_pkey PRIMARY KEY (id);


--
-- Name: recap_pacerhtmlfiles recap_pacerhtmlfiles_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerhtmlfiles
    ADD CONSTRAINT recap_pacerhtmlfiles_pkey PRIMARY KEY (id);


--
-- Name: recap_processingqueue recap_processingqueue_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT recap_processingqueue_pkey PRIMARY KEY (id);


--
-- Name: recap_rss_rssfeeddata recap_rss_rssfeeddata_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeeddata
    ADD CONSTRAINT recap_rss_rssfeeddata_pkey PRIMARY KEY (id);


--
-- Name: recap_rss_rssfeedstatus recap_rss_rssfeedstatus_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeedstatus
    ADD CONSTRAINT recap_rss_rssfeedstatus_pkey PRIMARY KEY (id);


--
-- Name: recap_rss_rssitemcache recap_rss_rssitemcache_hash_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssitemcache
    ADD CONSTRAINT recap_rss_rssitemcache_hash_key UNIQUE (hash);


--
-- Name: recap_rss_rssitemcache recap_rss_rssitemcache_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssitemcache
    ADD CONSTRAINT recap_rss_rssitemcache_pkey PRIMARY KEY (id);


--
-- Name: search_bankruptcyinformation search_bankruptcyinformation_docket_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformation
    ADD CONSTRAINT search_bankruptcyinformation_docket_id_key UNIQUE (docket_id);


--
-- Name: search_bankruptcyinformation search_bankruptcyinformation_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformation
    ADD CONSTRAINT search_bankruptcyinformation_pkey PRIMARY KEY (id);


--
-- Name: search_bankruptcyinformationevent search_bankruptcyinformationevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformationevent
    ADD CONSTRAINT search_bankruptcyinformationevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_citation search_citation_cluster_id_7a668830aad411f5_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citation
    ADD CONSTRAINT search_citation_cluster_id_7a668830aad411f5_uniq UNIQUE (cluster_id, volume, reporter, page);


--
-- Name: search_citation search_citation_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citation
    ADD CONSTRAINT search_citation_pkey PRIMARY KEY (id);


--
-- Name: search_citationevent search_citationevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citationevent
    ADD CONSTRAINT search_citationevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_claim search_claim_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim
    ADD CONSTRAINT search_claim_pkey PRIMARY KEY (id);


--
-- Name: search_claim_tags search_claim_tags_claim_id_tag_id_2f236693_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim_tags
    ADD CONSTRAINT search_claim_tags_claim_id_tag_id_2f236693_uniq UNIQUE (claim_id, tag_id);


--
-- Name: search_claim_tags search_claim_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim_tags
    ADD CONSTRAINT search_claim_tags_pkey PRIMARY KEY (id);


--
-- Name: search_claimevent search_claimevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimevent
    ADD CONSTRAINT search_claimevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_claimhistory search_claimhistory_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimhistory
    ADD CONSTRAINT search_claimhistory_pkey PRIMARY KEY (id);


--
-- Name: search_claimhistoryevent search_claimhistoryevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimhistoryevent
    ADD CONSTRAINT search_claimhistoryevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_claimtagsevent search_claimtagsevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimtagsevent
    ADD CONSTRAINT search_claimtagsevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_court_appeals_to search_court_appeals_to_from_court_id_to_court_id_006ed7af_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court_appeals_to
    ADD CONSTRAINT search_court_appeals_to_from_court_id_to_court_id_006ed7af_uniq UNIQUE (from_court_id, to_court_id);


--
-- Name: search_court_appeals_to search_court_appeals_to_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court_appeals_to
    ADD CONSTRAINT search_court_appeals_to_pkey PRIMARY KEY (id);


--
-- Name: search_court search_court_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court
    ADD CONSTRAINT search_court_pkey PRIMARY KEY (id);


--
-- Name: search_court search_court_position_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court
    ADD CONSTRAINT search_court_position_key UNIQUE ("position");


--
-- Name: search_courtappealstoevent search_courtappealstoevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courtappealstoevent
    ADD CONSTRAINT search_courtappealstoevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_courtevent search_courtevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courtevent
    ADD CONSTRAINT search_courtevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_courthouse search_courthouse_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courthouse
    ADD CONSTRAINT search_courthouse_pkey PRIMARY KEY (id);


--
-- Name: search_courthouseevent search_courthouseevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courthouseevent
    ADD CONSTRAINT search_courthouseevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_docket search_docket_idb_data_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docket_idb_data_id_key UNIQUE (idb_data_id);


--
-- Name: search_docket search_docket_originating_court_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docket_originating_court_id_key UNIQUE (originating_court_information_id);


--
-- Name: search_docket_panel search_docket_panel_docket_id_person_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_panel
    ADD CONSTRAINT search_docket_panel_docket_id_person_id_key UNIQUE (docket_id, person_id);


--
-- Name: search_docket_panel search_docket_panel_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_panel
    ADD CONSTRAINT search_docket_panel_pkey PRIMARY KEY (id);


--
-- Name: search_docket search_docket_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docket_pkey PRIMARY KEY (id);


--
-- Name: search_docket_tags search_docket_tags_docket_id_tag_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_tags
    ADD CONSTRAINT search_docket_tags_docket_id_tag_id_key UNIQUE (docket_id, tag_id);


--
-- Name: search_docket_tags search_docket_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_tags
    ADD CONSTRAINT search_docket_tags_pkey PRIMARY KEY (id);


--
-- Name: search_docketentry search_docketentry_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry
    ADD CONSTRAINT search_docketentry_pkey PRIMARY KEY (id);


--
-- Name: search_docketentry_tags search_docketentry_tags_docketentry_id_tag_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry_tags
    ADD CONSTRAINT search_docketentry_tags_docketentry_id_tag_id_key UNIQUE (docketentry_id, tag_id);


--
-- Name: search_docketentry_tags search_docketentry_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry_tags
    ADD CONSTRAINT search_docketentry_tags_pkey PRIMARY KEY (id);


--
-- Name: search_docketentryevent search_docketentryevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentryevent
    ADD CONSTRAINT search_docketentryevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_docketentrytagsevent search_docketentrytagsevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentrytagsevent
    ADD CONSTRAINT search_docketentrytagsevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_docketevent search_docketevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketevent
    ADD CONSTRAINT search_docketevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_docketpanelevent search_docketpanelevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketpanelevent
    ADD CONSTRAINT search_docketpanelevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_dockettagsevent search_dockettagsevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_dockettagsevent
    ADD CONSTRAINT search_dockettagsevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinion_joined_by search_opinion_joined_by_opinion_id_judge_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion_joined_by
    ADD CONSTRAINT search_opinion_joined_by_opinion_id_judge_id_key UNIQUE (opinion_id, person_id);


--
-- Name: search_opinion_joined_by search_opinion_joined_by_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion_joined_by
    ADD CONSTRAINT search_opinion_joined_by_pkey PRIMARY KEY (id);


--
-- Name: search_opinion search_opinion_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion
    ADD CONSTRAINT search_opinion_pkey PRIMARY KEY (id);


--
-- Name: search_opinioncluster_non_participating_judges search_opinioncluster_non_partic_opinioncluster_id_judge_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_non_participating_judges
    ADD CONSTRAINT search_opinioncluster_non_partic_opinioncluster_id_judge_id_key UNIQUE (opinioncluster_id, person_id);


--
-- Name: search_opinioncluster_non_participating_judges search_opinioncluster_non_participating_judges_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_non_participating_judges
    ADD CONSTRAINT search_opinioncluster_non_participating_judges_pkey PRIMARY KEY (id);


--
-- Name: search_opinioncluster_panel search_opinioncluster_panel_opinioncluster_id_judge_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_panel
    ADD CONSTRAINT search_opinioncluster_panel_opinioncluster_id_judge_id_key UNIQUE (opinioncluster_id, person_id);


--
-- Name: search_opinioncluster_panel search_opinioncluster_panel_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_panel
    ADD CONSTRAINT search_opinioncluster_panel_pkey PRIMARY KEY (id);


--
-- Name: search_opinioncluster search_opinioncluster_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster
    ADD CONSTRAINT search_opinioncluster_pkey PRIMARY KEY (id);


--
-- Name: search_opinionclusterevent search_opinionclusterevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusterevent
    ADD CONSTRAINT search_opinionclusterevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinionclusternonparticipatingjudgesevent search_opinionclusternonparticipatingjudgesevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusternonparticipatingjudgesevent
    ADD CONSTRAINT search_opinionclusternonparticipatingjudgesevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinionclusterpanelevent search_opinionclusterpanelevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionclusterpanelevent
    ADD CONSTRAINT search_opinionclusterpanelevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinionevent search_opinionevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionevent
    ADD CONSTRAINT search_opinionevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinionjoinedbyevent search_opinionjoinedbyevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionjoinedbyevent
    ADD CONSTRAINT search_opinionjoinedbyevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinionscited search_opinionscited_citing_opinion_id_7165e96b2aed974f_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscited
    ADD CONSTRAINT search_opinionscited_citing_opinion_id_7165e96b2aed974f_uniq UNIQUE (citing_opinion_id, cited_opinion_id);


--
-- Name: search_opinionscited search_opinionscited_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscited
    ADD CONSTRAINT search_opinionscited_pkey PRIMARY KEY (id);


--
-- Name: search_opinionscitedbyrecapdocument search_opinionscitedbyre_citing_document_id_cited_0c621cfd_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscitedbyrecapdocument
    ADD CONSTRAINT search_opinionscitedbyre_citing_document_id_cited_0c621cfd_uniq UNIQUE (citing_document_id, cited_opinion_id);


--
-- Name: search_opinionscitedbyrecapdocument search_opinionscitedbyrecapdocument_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscitedbyrecapdocument
    ADD CONSTRAINT search_opinionscitedbyrecapdocument_pkey PRIMARY KEY (id);


--
-- Name: search_originatingcourtinformation search_originatingcourtinformation_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformation
    ADD CONSTRAINT search_originatingcourtinformation_pkey PRIMARY KEY (id);


--
-- Name: search_originatingcourtinformationevent search_originatingcourtinformationevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformationevent
    ADD CONSTRAINT search_originatingcourtinformationevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_parenthetical search_parenthetical_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parenthetical
    ADD CONSTRAINT search_parenthetical_pkey PRIMARY KEY (id);


--
-- Name: search_parentheticalgroup search_parentheticalgroup_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parentheticalgroup
    ADD CONSTRAINT search_parentheticalgroup_pkey PRIMARY KEY (id);


--
-- Name: search_recapdocument search_recapdocument_docket_entry_id_37b2f4ece60cde00_uniq; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument
    ADD CONSTRAINT search_recapdocument_docket_entry_id_37b2f4ece60cde00_uniq UNIQUE (docket_entry_id, document_number, attachment_number);


--
-- Name: search_recapdocument search_recapdocument_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument
    ADD CONSTRAINT search_recapdocument_pkey PRIMARY KEY (id);


--
-- Name: search_recapdocument_tags search_recapdocument_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument_tags
    ADD CONSTRAINT search_recapdocument_tags_pkey PRIMARY KEY (id);


--
-- Name: search_recapdocument_tags search_recapdocument_tags_recapdocument_id_tag_id_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument_tags
    ADD CONSTRAINT search_recapdocument_tags_recapdocument_id_tag_id_key UNIQUE (recapdocument_id, tag_id);


--
-- Name: search_recapdocumentevent search_recapdocumentevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocumentevent
    ADD CONSTRAINT search_recapdocumentevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_recapdocumenttagsevent search_recapdocumenttagsevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocumenttagsevent
    ADD CONSTRAINT search_recapdocumenttagsevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_searchquery search_searchquery_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_searchquery
    ADD CONSTRAINT search_searchquery_pkey PRIMARY KEY (id);


--
-- Name: search_tag search_tag_name_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_tag
    ADD CONSTRAINT search_tag_name_key UNIQUE (name);


--
-- Name: search_tag search_tag_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_tag
    ADD CONSTRAINT search_tag_pkey PRIMARY KEY (id);


--
-- Name: search_tagevent search_tagevent_pkey; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_tagevent
    ADD CONSTRAINT search_tagevent_pkey PRIMARY KEY (pgh_id);


--
-- Name: search_opinion unique_opinion_ordering_key; Type: CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion
    ADD CONSTRAINT unique_opinion_ordering_key UNIQUE (cluster_id, ordering_key);


--
-- Name: audio_audio_03b47046; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_03b47046 ON public.audio_audio USING btree (local_path_original_file);


--
-- Name: audio_audio_0b869b2f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_0b869b2f ON public.audio_audio USING btree (date_blocked);


--
-- Name: audio_audio_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_1427d4ab ON public.audio_audio USING btree (docket_id);


--
-- Name: audio_audio_1cbcfc0f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_1cbcfc0f ON public.audio_audio USING btree (download_url);


--
-- Name: audio_audio_41ddbca9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_41ddbca9 ON public.audio_audio USING btree (local_path_mp3);


--
-- Name: audio_audio_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_5fdb3d66 ON public.audio_audio USING btree (date_modified);


--
-- Name: audio_audio_61326117; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_61326117 ON public.audio_audio USING btree (blocked);


--
-- Name: audio_audio_74a89174; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_74a89174 ON public.audio_audio USING btree (sha1);


--
-- Name: audio_audio_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_c69e55a4 ON public.audio_audio USING btree (date_created);


--
-- Name: audio_audio_download_url_44408fffeee4f71b_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_download_url_44408fffeee4f71b_like ON public.audio_audio USING btree (download_url varchar_pattern_ops);


--
-- Name: audio_audio_local_path_mp3_11f796e4872a9bad_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_local_path_mp3_11f796e4872a9bad_like ON public.audio_audio USING btree (local_path_mp3 varchar_pattern_ops);


--
-- Name: audio_audio_local_path_original_file_102ce483dde8495d_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_local_path_original_file_102ce483dde8495d_like ON public.audio_audio USING btree (local_path_original_file varchar_pattern_ops);


--
-- Name: audio_audio_panel_26f6023f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_panel_26f6023f ON public.audio_audio_panel USING btree (audio_id);


--
-- Name: audio_audio_panel_e7c5d788; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_panel_e7c5d788 ON public.audio_audio_panel USING btree (person_id);


--
-- Name: audio_audio_sha1_2510d5a8f56f35d4_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audio_sha1_2510d5a8f56f35d4_like ON public.audio_audio USING btree (sha1 varchar_pattern_ops);


--
-- Name: audio_audioevent_docket_id_d4acad63; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audioevent_docket_id_d4acad63 ON public.audio_audioevent USING btree (docket_id);


--
-- Name: audio_audioevent_pgh_context_id_f695da7c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audioevent_pgh_context_id_f695da7c ON public.audio_audioevent USING btree (pgh_context_id);


--
-- Name: audio_audioevent_pgh_obj_id_d4cc0c20; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audioevent_pgh_obj_id_d4cc0c20 ON public.audio_audioevent USING btree (pgh_obj_id);


--
-- Name: audio_audiopanelevent_audio_id_3aab9feb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audiopanelevent_audio_id_3aab9feb ON public.audio_audiopanelevent USING btree (audio_id);


--
-- Name: audio_audiopanelevent_person_id_0280e6c8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audiopanelevent_person_id_0280e6c8 ON public.audio_audiopanelevent USING btree (person_id);


--
-- Name: audio_audiopanelevent_pgh_context_id_5c5401fc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audiopanelevent_pgh_context_id_5c5401fc ON public.audio_audiopanelevent USING btree (pgh_context_id);


--
-- Name: audio_audiotranscriptionmetadata_audio_id_22f57b06; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX audio_audiotranscriptionmetadata_audio_id_22f57b06 ON public.audio_audiotranscriptionmetadata USING btree (audio_id);


--
-- Name: disclosures_agreement_date_created_799a50fa; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreement_date_created_799a50fa ON public.disclosures_agreement USING btree (date_created);


--
-- Name: disclosures_agreement_date_modified_cf46cbed; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreement_date_modified_cf46cbed ON public.disclosures_agreement USING btree (date_modified);


--
-- Name: disclosures_agreement_financial_disclosure_id_eb38358a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreement_financial_disclosure_id_eb38358a ON public.disclosures_agreement USING btree (financial_disclosure_id);


--
-- Name: disclosures_agreementevent_financial_disclosure_id_c846388c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreementevent_financial_disclosure_id_c846388c ON public.disclosures_agreementevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_agreementevent_pgh_context_id_b3cc1300; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreementevent_pgh_context_id_b3cc1300 ON public.disclosures_agreementevent USING btree (pgh_context_id);


--
-- Name: disclosures_agreementevent_pgh_obj_id_cdf3d4f8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_agreementevent_pgh_obj_id_cdf3d4f8 ON public.disclosures_agreementevent USING btree (pgh_obj_id);


--
-- Name: disclosures_debt_date_created_ed9d5440; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debt_date_created_ed9d5440 ON public.disclosures_debt USING btree (date_created);


--
-- Name: disclosures_debt_date_modified_a1482a62; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debt_date_modified_a1482a62 ON public.disclosures_debt USING btree (date_modified);


--
-- Name: disclosures_debt_financial_disclosure_id_18a78f4c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debt_financial_disclosure_id_18a78f4c ON public.disclosures_debt USING btree (financial_disclosure_id);


--
-- Name: disclosures_debtevent_financial_disclosure_id_98538f65; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debtevent_financial_disclosure_id_98538f65 ON public.disclosures_debtevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_debtevent_pgh_context_id_651400a2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debtevent_pgh_context_id_651400a2 ON public.disclosures_debtevent USING btree (pgh_context_id);


--
-- Name: disclosures_debtevent_pgh_obj_id_11b68656; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_debtevent_pgh_obj_id_11b68656 ON public.disclosures_debtevent USING btree (pgh_obj_id);


--
-- Name: disclosures_financialdisclosure_date_created_85a1e80e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_date_created_85a1e80e ON public.disclosures_financialdisclosure USING btree (date_created);


--
-- Name: disclosures_financialdisclosure_date_modified_717ae8fa; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_date_modified_717ae8fa ON public.disclosures_financialdisclosure USING btree (date_modified);


--
-- Name: disclosures_financialdisclosure_filepath_8266edc6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_filepath_8266edc6 ON public.disclosures_financialdisclosure USING btree (filepath);


--
-- Name: disclosures_financialdisclosure_filepath_8266edc6_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_filepath_8266edc6_like ON public.disclosures_financialdisclosure USING btree (filepath varchar_pattern_ops);


--
-- Name: disclosures_financialdisclosure_person_id_83e04c6c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_person_id_83e04c6c ON public.disclosures_financialdisclosure USING btree (person_id);


--
-- Name: disclosures_financialdisclosure_sha1_552f12ae_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_sha1_552f12ae_like ON public.disclosures_financialdisclosure USING btree (sha1 varchar_pattern_ops);


--
-- Name: disclosures_financialdisclosure_year_ee032263; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosure_year_ee032263 ON public.disclosures_financialdisclosure USING btree (year);


--
-- Name: disclosures_financialdisclosureevent_person_id_6936f8d9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosureevent_person_id_6936f8d9 ON public.disclosures_financialdisclosureevent USING btree (person_id);


--
-- Name: disclosures_financialdisclosureevent_pgh_context_id_83781350; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosureevent_pgh_context_id_83781350 ON public.disclosures_financialdisclosureevent USING btree (pgh_context_id);


--
-- Name: disclosures_financialdisclosureevent_pgh_obj_id_c4ffefde; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_financialdisclosureevent_pgh_obj_id_c4ffefde ON public.disclosures_financialdisclosureevent USING btree (pgh_obj_id);


--
-- Name: disclosures_gift_date_created_c3e030fc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_gift_date_created_c3e030fc ON public.disclosures_gift USING btree (date_created);


--
-- Name: disclosures_gift_date_modified_ceb7453c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_gift_date_modified_ceb7453c ON public.disclosures_gift USING btree (date_modified);


--
-- Name: disclosures_gift_financial_disclosure_id_67efabf6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_gift_financial_disclosure_id_67efabf6 ON public.disclosures_gift USING btree (financial_disclosure_id);


--
-- Name: disclosures_giftevent_financial_disclosure_id_6da98fc4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_giftevent_financial_disclosure_id_6da98fc4 ON public.disclosures_giftevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_giftevent_pgh_context_id_e35c6eb6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_giftevent_pgh_context_id_e35c6eb6 ON public.disclosures_giftevent USING btree (pgh_context_id);


--
-- Name: disclosures_giftevent_pgh_obj_id_59ce33d3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_giftevent_pgh_obj_id_59ce33d3 ON public.disclosures_giftevent USING btree (pgh_obj_id);


--
-- Name: disclosures_investment_date_created_252beaa5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investment_date_created_252beaa5 ON public.disclosures_investment USING btree (date_created);


--
-- Name: disclosures_investment_date_modified_e2f8f841; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investment_date_modified_e2f8f841 ON public.disclosures_investment USING btree (date_modified);


--
-- Name: disclosures_investment_financial_disclosure_id_ad904849; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investment_financial_disclosure_id_ad904849 ON public.disclosures_investment USING btree (financial_disclosure_id);


--
-- Name: disclosures_investmentevent_financial_disclosure_id_1692a34a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investmentevent_financial_disclosure_id_1692a34a ON public.disclosures_investmentevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_investmentevent_pgh_context_id_d04fb495; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investmentevent_pgh_context_id_d04fb495 ON public.disclosures_investmentevent USING btree (pgh_context_id);


--
-- Name: disclosures_investmentevent_pgh_obj_id_f8d5278d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_investmentevent_pgh_obj_id_f8d5278d ON public.disclosures_investmentevent USING btree (pgh_obj_id);


--
-- Name: disclosures_noninvestmenti_financial_disclosure_id_5b351795; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmenti_financial_disclosure_id_5b351795 ON public.disclosures_noninvestmentincome USING btree (financial_disclosure_id);


--
-- Name: disclosures_noninvestmenti_financial_disclosure_id_9b4f08af; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmenti_financial_disclosure_id_9b4f08af ON public.disclosures_noninvestmentincomeevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_noninvestmentincome_date_created_d876ac4b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmentincome_date_created_d876ac4b ON public.disclosures_noninvestmentincome USING btree (date_created);


--
-- Name: disclosures_noninvestmentincome_date_modified_d3c68c8b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmentincome_date_modified_d3c68c8b ON public.disclosures_noninvestmentincome USING btree (date_modified);


--
-- Name: disclosures_noninvestmentincomeevent_pgh_context_id_0497f781; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmentincomeevent_pgh_context_id_0497f781 ON public.disclosures_noninvestmentincomeevent USING btree (pgh_context_id);


--
-- Name: disclosures_noninvestmentincomeevent_pgh_obj_id_a32a87a9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_noninvestmentincomeevent_pgh_obj_id_a32a87a9 ON public.disclosures_noninvestmentincomeevent USING btree (pgh_obj_id);


--
-- Name: disclosures_position_date_created_e515f4be; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_position_date_created_e515f4be ON public.disclosures_position USING btree (date_created);


--
-- Name: disclosures_position_date_modified_01fafcba; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_position_date_modified_01fafcba ON public.disclosures_position USING btree (date_modified);


--
-- Name: disclosures_position_financial_disclosure_id_b81030c0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_position_financial_disclosure_id_b81030c0 ON public.disclosures_position USING btree (financial_disclosure_id);


--
-- Name: disclosures_positionevent_financial_disclosure_id_ad3bcb32; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_positionevent_financial_disclosure_id_ad3bcb32 ON public.disclosures_positionevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_positionevent_pgh_context_id_f5ae70f1; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_positionevent_pgh_context_id_f5ae70f1 ON public.disclosures_positionevent USING btree (pgh_context_id);


--
-- Name: disclosures_positionevent_pgh_obj_id_41aaca6a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_positionevent_pgh_obj_id_41aaca6a ON public.disclosures_positionevent USING btree (pgh_obj_id);


--
-- Name: disclosures_reimbursement_date_created_4056c28a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursement_date_created_4056c28a ON public.disclosures_reimbursement USING btree (date_created);


--
-- Name: disclosures_reimbursement_date_modified_3ca21f45; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursement_date_modified_3ca21f45 ON public.disclosures_reimbursement USING btree (date_modified);


--
-- Name: disclosures_reimbursement_financial_disclosure_id_141ee670; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursement_financial_disclosure_id_141ee670 ON public.disclosures_reimbursement USING btree (financial_disclosure_id);


--
-- Name: disclosures_reimbursementevent_financial_disclosure_id_c3e98e6a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursementevent_financial_disclosure_id_c3e98e6a ON public.disclosures_reimbursementevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_reimbursementevent_pgh_context_id_bc88ae92; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursementevent_pgh_context_id_bc88ae92 ON public.disclosures_reimbursementevent USING btree (pgh_context_id);


--
-- Name: disclosures_reimbursementevent_pgh_obj_id_19ad4423; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_reimbursementevent_pgh_obj_id_19ad4423 ON public.disclosures_reimbursementevent USING btree (pgh_obj_id);


--
-- Name: disclosures_spouseincome_date_created_00632eb5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincome_date_created_00632eb5 ON public.disclosures_spouseincome USING btree (date_created);


--
-- Name: disclosures_spouseincome_date_modified_9bea7dd2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincome_date_modified_9bea7dd2 ON public.disclosures_spouseincome USING btree (date_modified);


--
-- Name: disclosures_spouseincome_financial_disclosure_id_94e0c727; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincome_financial_disclosure_id_94e0c727 ON public.disclosures_spouseincome USING btree (financial_disclosure_id);


--
-- Name: disclosures_spouseincomeevent_financial_disclosure_id_c0c5aac5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincomeevent_financial_disclosure_id_c0c5aac5 ON public.disclosures_spouseincomeevent USING btree (financial_disclosure_id);


--
-- Name: disclosures_spouseincomeevent_pgh_context_id_cc56881c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincomeevent_pgh_context_id_cc56881c ON public.disclosures_spouseincomeevent USING btree (pgh_context_id);


--
-- Name: disclosures_spouseincomeevent_pgh_obj_id_35eed7df; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX disclosures_spouseincomeevent_pgh_obj_id_35eed7df ON public.disclosures_spouseincomeevent USING btree (pgh_obj_id);


--
-- Name: district_court_docket_lookup_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX district_court_docket_lookup_idx ON public.search_docket USING btree (court_id, docket_number_core, pacer_case_id);


--
-- Name: entry_number_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX entry_number_idx ON public.search_docketentry USING btree (docket_id, entry_number) WHERE (entry_number = 1);


--
-- Name: pacer_doc_id_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX pacer_doc_id_idx ON public.search_recapdocument USING btree (pacer_doc_id) WHERE (NOT ((pacer_doc_id)::text = ''::text));


--
-- Name: people_db_abarating_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abarating_5fdb3d66 ON public.people_db_abarating USING btree (date_modified);


--
-- Name: people_db_abarating_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abarating_a8452ca7 ON public.people_db_abarating USING btree (person_id);


--
-- Name: people_db_abarating_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abarating_c69e55a4 ON public.people_db_abarating USING btree (date_created);


--
-- Name: people_db_abaratingevent_person_id_976485e8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abaratingevent_person_id_976485e8 ON public.people_db_abaratingevent USING btree (person_id);


--
-- Name: people_db_abaratingevent_pgh_context_id_60d3496a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abaratingevent_pgh_context_id_60d3496a ON public.people_db_abaratingevent USING btree (pgh_context_id);


--
-- Name: people_db_abaratingevent_pgh_obj_id_0e6a9bc3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_abaratingevent_pgh_obj_id_0e6a9bc3 ON public.people_db_abaratingevent USING btree (pgh_obj_id);


--
-- Name: people_db_attorney_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorney_5fdb3d66 ON public.people_db_attorney USING btree (date_modified);


--
-- Name: people_db_attorney_b068931c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorney_b068931c ON public.people_db_attorney USING btree (name);


--
-- Name: people_db_attorney_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorney_c69e55a4 ON public.people_db_attorney USING btree (date_created);


--
-- Name: people_db_attorney_name_46d318a02757f6dd_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorney_name_46d318a02757f6dd_like ON public.people_db_attorney USING btree (name text_pattern_ops);


--
-- Name: people_db_attorneyorganization_0c0ae404; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_0c0ae404 ON public.people_db_attorneyorganization USING btree (zip_code);


--
-- Name: people_db_attorneyorganization_4ed5d2ea; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_4ed5d2ea ON public.people_db_attorneyorganization USING btree (city);


--
-- Name: people_db_attorneyorganization_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_5fdb3d66 ON public.people_db_attorneyorganization USING btree (date_modified);


--
-- Name: people_db_attorneyorganization_81e70cb1; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_81e70cb1 ON public.people_db_attorneyorganization USING btree (address1);


--
-- Name: people_db_attorneyorganization_9ed39e2e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_9ed39e2e ON public.people_db_attorneyorganization USING btree (state);


--
-- Name: people_db_attorneyorganization_address1_403814857b548870_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_address1_403814857b548870_like ON public.people_db_attorneyorganization USING btree (address1 text_pattern_ops);


--
-- Name: people_db_attorneyorganization_address2_403814857b4403d3_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_address2_403814857b4403d3_like ON public.people_db_attorneyorganization USING btree (address2 text_pattern_ops);


--
-- Name: people_db_attorneyorganization_b068931c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_b068931c ON public.people_db_attorneyorganization USING btree (name);


--
-- Name: people_db_attorneyorganization_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_c69e55a4 ON public.people_db_attorneyorganization USING btree (date_created);


--
-- Name: people_db_attorneyorganization_city_2a5498a19c997008_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_city_2a5498a19c997008_like ON public.people_db_attorneyorganization USING btree (city text_pattern_ops);


--
-- Name: people_db_attorneyorganization_f669f8e9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_f669f8e9 ON public.people_db_attorneyorganization USING btree (address2);


--
-- Name: people_db_attorneyorganization_lookup_key_4723770634841e39_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_lookup_key_4723770634841e39_like ON public.people_db_attorneyorganization USING btree (lookup_key text_pattern_ops);


--
-- Name: people_db_attorneyorganization_name_eddcefc5b671344_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_name_eddcefc5b671344_like ON public.people_db_attorneyorganization USING btree (name text_pattern_ops);


--
-- Name: people_db_attorneyorganization_state_18fe99f5bf671255_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_state_18fe99f5bf671255_like ON public.people_db_attorneyorganization USING btree (state varchar_pattern_ops);


--
-- Name: people_db_attorneyorganization_zip_code_66632293e4093e4e_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganization_zip_code_66632293e4093e4e_like ON public.people_db_attorneyorganization USING btree (zip_code varchar_pattern_ops);


--
-- Name: people_db_attorneyorganizationassociation_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganizationassociation_1427d4ab ON public.people_db_attorneyorganizationassociation USING btree (docket_id);


--
-- Name: people_db_attorneyorganizationassociation_9f99f769; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganizationassociation_9f99f769 ON public.people_db_attorneyorganizationassociation USING btree (attorney_id);


--
-- Name: people_db_attorneyorganizationassociation_a2c8783d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_attorneyorganizationassociation_a2c8783d ON public.people_db_attorneyorganizationassociation USING btree (attorney_organization_id);


--
-- Name: people_db_criminalcomplaint_4edc179c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_criminalcomplaint_4edc179c ON public.people_db_criminalcomplaint USING btree (party_type_id);


--
-- Name: people_db_criminalcount_4edc179c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_criminalcount_4edc179c ON public.people_db_criminalcount USING btree (party_type_id);


--
-- Name: people_db_education_5fc7164b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_education_5fc7164b ON public.people_db_education USING btree (school_id);


--
-- Name: people_db_education_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_education_5fdb3d66 ON public.people_db_education USING btree (date_modified);


--
-- Name: people_db_education_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_education_a8452ca7 ON public.people_db_education USING btree (person_id);


--
-- Name: people_db_education_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_education_c69e55a4 ON public.people_db_education USING btree (date_created);


--
-- Name: people_db_educationevent_person_id_86892be3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_educationevent_person_id_86892be3 ON public.people_db_educationevent USING btree (person_id);


--
-- Name: people_db_educationevent_pgh_context_id_93dac561; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_educationevent_pgh_context_id_93dac561 ON public.people_db_educationevent USING btree (pgh_context_id);


--
-- Name: people_db_educationevent_pgh_obj_id_242c5dea; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_educationevent_pgh_obj_id_242c5dea ON public.people_db_educationevent USING btree (pgh_obj_id);


--
-- Name: people_db_educationevent_school_id_5d83b038; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_educationevent_school_id_5d83b038 ON public.people_db_educationevent USING btree (school_id);


--
-- Name: people_db_party_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_5fdb3d66 ON public.people_db_party USING btree (date_modified);


--
-- Name: people_db_party_b068931c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_b068931c ON public.people_db_party USING btree (name);


--
-- Name: people_db_party_bb2cb5a6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_bb2cb5a6 ON public.people_db_party USING btree (extra_info);


--
-- Name: people_db_party_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_c69e55a4 ON public.people_db_party USING btree (date_created);


--
-- Name: people_db_party_extra_info_7a50cbcc44fd5d7a_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_extra_info_7a50cbcc44fd5d7a_like ON public.people_db_party USING btree (extra_info text_pattern_ops);


--
-- Name: people_db_party_name_3d12b76fe7f0ae1c_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_party_name_3d12b76fe7f0ae1c_like ON public.people_db_party USING btree (name text_pattern_ops);


--
-- Name: people_db_partytype_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_partytype_1427d4ab ON public.people_db_partytype USING btree (docket_id);


--
-- Name: people_db_partytype_2c662395; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_partytype_2c662395 ON public.people_db_partytype USING btree (party_id);


--
-- Name: people_db_partytype_b068931c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_partytype_b068931c ON public.people_db_partytype USING btree (name);


--
-- Name: people_db_partytype_bb2cb5a6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_partytype_bb2cb5a6 ON public.people_db_partytype USING btree (extra_info);


--
-- Name: people_db_partytype_name_4cb81702de26fee8_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_partytype_name_4cb81702de26fee8_like ON public.people_db_partytype USING btree (name varchar_pattern_ops);


--
-- Name: people_db_person_2dbcba41; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_2dbcba41 ON public.people_db_person USING btree (slug);


--
-- Name: people_db_person_3c6ec45e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_3c6ec45e ON public.people_db_person USING btree (is_alias_of_id);


--
-- Name: people_db_person_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_5fdb3d66 ON public.people_db_person USING btree (date_modified);


--
-- Name: people_db_person_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_c69e55a4 ON public.people_db_person USING btree (date_created);


--
-- Name: people_db_person_c943ec03; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_c943ec03 ON public.people_db_person USING btree (name_last);


--
-- Name: people_db_person_e489b049; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_e489b049 ON public.people_db_person USING btree (ftm_total_received);


--
-- Name: people_db_person_name_last_2c38f190d58b7eba_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_name_last_2c38f190d58b7eba_like ON public.people_db_person USING btree (name_last varchar_pattern_ops);


--
-- Name: people_db_person_race_3f2f3687; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_race_3f2f3687 ON public.people_db_person_race USING btree (race_id);


--
-- Name: people_db_person_race_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_race_a8452ca7 ON public.people_db_person_race USING btree (person_id);


--
-- Name: people_db_person_slug_10074d278f243e42_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_person_slug_10074d278f243e42_like ON public.people_db_person USING btree (slug varchar_pattern_ops);


--
-- Name: people_db_personevent_is_alias_of_id_dff0de5e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personevent_is_alias_of_id_dff0de5e ON public.people_db_personevent USING btree (is_alias_of_id);


--
-- Name: people_db_personevent_pgh_context_id_8c18edc2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personevent_pgh_context_id_8c18edc2 ON public.people_db_personevent USING btree (pgh_context_id);


--
-- Name: people_db_personevent_pgh_obj_id_3a44721c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personevent_pgh_obj_id_3a44721c ON public.people_db_personevent USING btree (pgh_obj_id);


--
-- Name: people_db_personraceevent_person_id_000fffe6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personraceevent_person_id_000fffe6 ON public.people_db_personraceevent USING btree (person_id);


--
-- Name: people_db_personraceevent_pgh_context_id_6e61479f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personraceevent_pgh_context_id_6e61479f ON public.people_db_personraceevent USING btree (pgh_context_id);


--
-- Name: people_db_personraceevent_race_id_ec19c576; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_personraceevent_race_id_ec19c576 ON public.people_db_personraceevent USING btree (race_id);


--
-- Name: people_db_politicalaffiliation_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliation_5fdb3d66 ON public.people_db_politicalaffiliation USING btree (date_modified);


--
-- Name: people_db_politicalaffiliation_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliation_a8452ca7 ON public.people_db_politicalaffiliation USING btree (person_id);


--
-- Name: people_db_politicalaffiliation_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliation_c69e55a4 ON public.people_db_politicalaffiliation USING btree (date_created);


--
-- Name: people_db_politicalaffiliationevent_person_id_968b07ce; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliationevent_person_id_968b07ce ON public.people_db_politicalaffiliationevent USING btree (person_id);


--
-- Name: people_db_politicalaffiliationevent_pgh_context_id_dfbcdb75; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliationevent_pgh_context_id_dfbcdb75 ON public.people_db_politicalaffiliationevent USING btree (pgh_context_id);


--
-- Name: people_db_politicalaffiliationevent_pgh_obj_id_c98e3cf6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_politicalaffiliationevent_pgh_obj_id_c98e3cf6 ON public.people_db_politicalaffiliationevent USING btree (pgh_obj_id);


--
-- Name: people_db_position_0ce67364; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_0ce67364 ON public.people_db_position USING btree (date_start);


--
-- Name: people_db_position_1301727a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_1301727a ON public.people_db_position USING btree (date_elected);


--
-- Name: people_db_position_22b8ff35; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_22b8ff35 ON public.people_db_position USING btree (appointer_id);


--
-- Name: people_db_position_2a1c0b55; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_2a1c0b55 ON public.people_db_position USING btree (date_confirmation);


--
-- Name: people_db_position_43e12164; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_43e12164 ON public.people_db_position USING btree (date_judicial_committee_action);


--
-- Name: people_db_position_5fc7164b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_5fc7164b ON public.people_db_position USING btree (school_id);


--
-- Name: people_db_position_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_5fdb3d66 ON public.people_db_position USING btree (date_modified);


--
-- Name: people_db_position_7a183bc6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_7a183bc6 ON public.people_db_position USING btree (date_recess_appointment);


--
-- Name: people_db_position_7a46e69c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_7a46e69c ON public.people_db_position USING btree (court_id);


--
-- Name: people_db_position_a2289cae; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_a2289cae ON public.people_db_position USING btree (date_referred_to_judicial_committee);


--
-- Name: people_db_position_a7ad19f8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_a7ad19f8 ON public.people_db_position USING btree (date_nominated);


--
-- Name: people_db_position_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_a8452ca7 ON public.people_db_position USING btree (person_id);


--
-- Name: people_db_position_a9962d2d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_a9962d2d ON public.people_db_position USING btree (date_retirement);


--
-- Name: people_db_position_b070f947; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_b070f947 ON public.people_db_position USING btree (predecessor_id);


--
-- Name: people_db_position_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_c69e55a4 ON public.people_db_position USING btree (date_created);


--
-- Name: people_db_position_court_id_7141eee9b516a894_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_court_id_7141eee9b516a894_like ON public.people_db_position USING btree (court_id varchar_pattern_ops);


--
-- Name: people_db_position_d32bfe21; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_d32bfe21 ON public.people_db_position USING btree (date_termination);


--
-- Name: people_db_position_eae0a89e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_eae0a89e ON public.people_db_position USING btree (supervisor_id);


--
-- Name: people_db_position_ed551732; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_position_ed551732 ON public.people_db_position USING btree (date_hearing);


--
-- Name: people_db_positionevent_appointer_id_9a1a141d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_appointer_id_9a1a141d ON public.people_db_positionevent USING btree (appointer_id);


--
-- Name: people_db_positionevent_court_id_c27f27bf; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_court_id_c27f27bf ON public.people_db_positionevent USING btree (court_id);


--
-- Name: people_db_positionevent_court_id_c27f27bf_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_court_id_c27f27bf_like ON public.people_db_positionevent USING btree (court_id varchar_pattern_ops);


--
-- Name: people_db_positionevent_person_id_b48e5d5c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_person_id_b48e5d5c ON public.people_db_positionevent USING btree (person_id);


--
-- Name: people_db_positionevent_pgh_context_id_91818e04; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_pgh_context_id_91818e04 ON public.people_db_positionevent USING btree (pgh_context_id);


--
-- Name: people_db_positionevent_pgh_obj_id_e37b1a99; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_pgh_obj_id_e37b1a99 ON public.people_db_positionevent USING btree (pgh_obj_id);


--
-- Name: people_db_positionevent_predecessor_id_a183a0e5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_predecessor_id_a183a0e5 ON public.people_db_positionevent USING btree (predecessor_id);


--
-- Name: people_db_positionevent_school_id_8435314e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_school_id_8435314e ON public.people_db_positionevent USING btree (school_id);


--
-- Name: people_db_positionevent_supervisor_id_d1b31dbb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_positionevent_supervisor_id_d1b31dbb ON public.people_db_positionevent USING btree (supervisor_id);


--
-- Name: people_db_raceevent_pgh_context_id_590006cf; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_raceevent_pgh_context_id_590006cf ON public.people_db_raceevent USING btree (pgh_context_id);


--
-- Name: people_db_raceevent_pgh_obj_id_f7ec57e8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_raceevent_pgh_obj_id_f7ec57e8 ON public.people_db_raceevent USING btree (pgh_obj_id);


--
-- Name: people_db_retentionevent_4e6b5ce9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentionevent_4e6b5ce9 ON public.people_db_retentionevent USING btree (date_retention);


--
-- Name: people_db_retentionevent_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentionevent_5fdb3d66 ON public.people_db_retentionevent USING btree (date_modified);


--
-- Name: people_db_retentionevent_bce5bd07; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentionevent_bce5bd07 ON public.people_db_retentionevent USING btree (position_id);


--
-- Name: people_db_retentionevent_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentionevent_c69e55a4 ON public.people_db_retentionevent USING btree (date_created);


--
-- Name: people_db_retentioneventevent_pgh_context_id_81f7850b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentioneventevent_pgh_context_id_81f7850b ON public.people_db_retentioneventevent USING btree (pgh_context_id);


--
-- Name: people_db_retentioneventevent_pgh_obj_id_902d74ea; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentioneventevent_pgh_obj_id_902d74ea ON public.people_db_retentioneventevent USING btree (pgh_obj_id);


--
-- Name: people_db_retentioneventevent_position_id_96c16566; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_retentioneventevent_position_id_96c16566 ON public.people_db_retentioneventevent USING btree (position_id);


--
-- Name: people_db_role_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_role_1427d4ab ON public.people_db_role USING btree (docket_id);


--
-- Name: people_db_role_29a7e964; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_role_29a7e964 ON public.people_db_role USING btree (role);


--
-- Name: people_db_role_2c662395; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_role_2c662395 ON public.people_db_role USING btree (party_id);


--
-- Name: people_db_role_9f99f769; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_role_9f99f769 ON public.people_db_role USING btree (attorney_id);


--
-- Name: people_db_school_3c6ec45e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_3c6ec45e ON public.people_db_school USING btree (is_alias_of_id);


--
-- Name: people_db_school_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_5fdb3d66 ON public.people_db_school USING btree (date_modified);


--
-- Name: people_db_school_b068931c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_b068931c ON public.people_db_school USING btree (name);


--
-- Name: people_db_school_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_c69e55a4 ON public.people_db_school USING btree (date_created);


--
-- Name: people_db_school_ffef75ef; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_ffef75ef ON public.people_db_school USING btree (ein);


--
-- Name: people_db_school_name_55359da037ff6cd5_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_school_name_55359da037ff6cd5_like ON public.people_db_school USING btree (name varchar_pattern_ops);


--
-- Name: people_db_schoolevent_is_alias_of_id_4c4332d7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_schoolevent_is_alias_of_id_4c4332d7 ON public.people_db_schoolevent USING btree (is_alias_of_id);


--
-- Name: people_db_schoolevent_pgh_context_id_b32512bd; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_schoolevent_pgh_context_id_b32512bd ON public.people_db_schoolevent USING btree (pgh_context_id);


--
-- Name: people_db_schoolevent_pgh_obj_id_57bf2a67; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_schoolevent_pgh_obj_id_57bf2a67 ON public.people_db_schoolevent USING btree (pgh_obj_id);


--
-- Name: people_db_source_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_source_5fdb3d66 ON public.people_db_source USING btree (date_modified);


--
-- Name: people_db_source_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_source_a8452ca7 ON public.people_db_source USING btree (person_id);


--
-- Name: people_db_source_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_source_c69e55a4 ON public.people_db_source USING btree (date_created);


--
-- Name: people_db_sourceevent_person_id_f32cf8b3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_sourceevent_person_id_f32cf8b3 ON public.people_db_sourceevent USING btree (person_id);


--
-- Name: people_db_sourceevent_pgh_context_id_6bf9fb8f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_sourceevent_pgh_context_id_6bf9fb8f ON public.people_db_sourceevent USING btree (pgh_context_id);


--
-- Name: people_db_sourceevent_pgh_obj_id_c37b1d95; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX people_db_sourceevent_pgh_obj_id_c37b1d95 ON public.people_db_sourceevent USING btree (pgh_obj_id);


--
-- Name: recap_emailprocessingqueue_court_id_83f67bf3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_court_id_83f67bf3 ON public.recap_emailprocessingqueue USING btree (court_id);


--
-- Name: recap_emailprocessingqueue_court_id_83f67bf3_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_court_id_83f67bf3_like ON public.recap_emailprocessingqueue USING btree (court_id varchar_pattern_ops);


--
-- Name: recap_emailprocessingqueue_date_created_2f32c34d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_date_created_2f32c34d ON public.recap_emailprocessingqueue USING btree (date_created);


--
-- Name: recap_emailprocessingqueue_date_modified_0900d415; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_date_modified_0900d415 ON public.recap_emailprocessingqueue USING btree (date_modified);


--
-- Name: recap_emailprocessingqueue_emailprocessingqueue_id_896acbad; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_emailprocessingqueue_id_896acbad ON public.recap_emailprocessingqueue_recap_documents USING btree (emailprocessingqueue_id);


--
-- Name: recap_emailprocessingqueue_recapdocument_id_66e16cbf; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_recapdocument_id_66e16cbf ON public.recap_emailprocessingqueue_recap_documents USING btree (recapdocument_id);


--
-- Name: recap_emailprocessingqueue_status_798f5968; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_status_798f5968 ON public.recap_emailprocessingqueue USING btree (status);


--
-- Name: recap_emailprocessingqueue_uploader_id_32651a93; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_emailprocessingqueue_uploader_id_32651a93 ON public.recap_emailprocessingqueue USING btree (uploader_id);


--
-- Name: recap_fjcintegrateddatabase_3acc614b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_3acc614b ON public.recap_fjcintegrateddatabase USING btree (circuit_id);


--
-- Name: recap_fjcintegrateddatabase_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_5fdb3d66 ON public.recap_fjcintegrateddatabase USING btree (date_modified);


--
-- Name: recap_fjcintegrateddatabase_98cc5f59; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_98cc5f59 ON public.recap_fjcintegrateddatabase USING btree (date_filed);


--
-- Name: recap_fjcintegrateddatabase_a34a99d3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_a34a99d3 ON public.recap_fjcintegrateddatabase USING btree (district_id);


--
-- Name: recap_fjcintegrateddatabase_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_c69e55a4 ON public.recap_fjcintegrateddatabase USING btree (date_created);


--
-- Name: recap_fjcintegrateddatabase_circuit_id_6c29143a8524f734_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_circuit_id_6c29143a8524f734_like ON public.recap_fjcintegrateddatabase USING btree (circuit_id varchar_pattern_ops);


--
-- Name: recap_fjcintegrateddatabase_defendant_1917b677dad9291b_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_defendant_1917b677dad9291b_uniq ON public.recap_fjcintegrateddatabase USING btree (defendant);


--
-- Name: recap_fjcintegrateddatabase_district_id_422635be62a49e48_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_district_id_422635be62a49e48_like ON public.recap_fjcintegrateddatabase USING btree (district_id varchar_pattern_ops);


--
-- Name: recap_fjcintegrateddatabase_district_id_455568623a9da568_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_district_id_455568623a9da568_idx ON public.recap_fjcintegrateddatabase USING btree (district_id, docket_number);


--
-- Name: recap_fjcintegrateddatabase_plaintiff_3aba2127efcb646f_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_plaintiff_3aba2127efcb646f_uniq ON public.recap_fjcintegrateddatabase USING btree (plaintiff);


--
-- Name: recap_fjcintegrateddatabase_section_73d9a83f15a49cf7_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_section_73d9a83f15a49cf7_uniq ON public.recap_fjcintegrateddatabase USING btree (section);


--
-- Name: recap_fjcintegrateddatabase_subsection_6d4a1e238eb3d830_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_subsection_6d4a1e238eb3d830_uniq ON public.recap_fjcintegrateddatabase USING btree (subsection);


--
-- Name: recap_fjcintegrateddatabase_title_237ea2ba12345066_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_fjcintegrateddatabase_title_237ea2ba12345066_uniq ON public.recap_fjcintegrateddatabase USING btree (title);


--
-- Name: recap_pacerfetchqueue_court_id_1246ddd3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_court_id_1246ddd3 ON public.recap_pacerfetchqueue USING btree (court_id);


--
-- Name: recap_pacerfetchqueue_court_id_1246ddd3_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_court_id_1246ddd3_like ON public.recap_pacerfetchqueue USING btree (court_id varchar_pattern_ops);


--
-- Name: recap_pacerfetchqueue_date_completed_cfc17415; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_date_completed_cfc17415 ON public.recap_pacerfetchqueue USING btree (date_completed);


--
-- Name: recap_pacerfetchqueue_date_created_e21b4d2e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_date_created_e21b4d2e ON public.recap_pacerfetchqueue USING btree (date_created);


--
-- Name: recap_pacerfetchqueue_date_modified_d110c824; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_date_modified_d110c824 ON public.recap_pacerfetchqueue USING btree (date_modified);


--
-- Name: recap_pacerfetchqueue_docket_id_371bfcf0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_docket_id_371bfcf0 ON public.recap_pacerfetchqueue USING btree (docket_id);


--
-- Name: recap_pacerfetchqueue_pacer_case_id_21aa36c3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_pacer_case_id_21aa36c3 ON public.recap_pacerfetchqueue USING btree (pacer_case_id);


--
-- Name: recap_pacerfetchqueue_pacer_case_id_21aa36c3_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_pacer_case_id_21aa36c3_like ON public.recap_pacerfetchqueue USING btree (pacer_case_id varchar_pattern_ops);


--
-- Name: recap_pacerfetchqueue_recap_document_id_b9c23829; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_recap_document_id_b9c23829 ON public.recap_pacerfetchqueue USING btree (recap_document_id);


--
-- Name: recap_pacerfetchqueue_status_19964cb1; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_status_19964cb1 ON public.recap_pacerfetchqueue USING btree (status);


--
-- Name: recap_pacerfetchqueue_user_id_a2c0c6f8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerfetchqueue_user_id_a2c0c6f8 ON public.recap_pacerfetchqueue USING btree (user_id);


--
-- Name: recap_pacerhtmlfiles_417f1b1c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerhtmlfiles_417f1b1c ON public.recap_pacerhtmlfiles USING btree (content_type_id);


--
-- Name: recap_pacerhtmlfiles_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerhtmlfiles_5fdb3d66 ON public.recap_pacerhtmlfiles USING btree (date_modified);


--
-- Name: recap_pacerhtmlfiles_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_pacerhtmlfiles_c69e55a4 ON public.recap_pacerhtmlfiles USING btree (date_created);


--
-- Name: recap_processingqueue_11298d03; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_11298d03 ON public.recap_processingqueue USING btree (recap_document_id);


--
-- Name: recap_processingqueue_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_1427d4ab ON public.recap_processingqueue USING btree (docket_id);


--
-- Name: recap_processingqueue_40d913b2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_40d913b2 ON public.recap_processingqueue USING btree (docket_entry_id);


--
-- Name: recap_processingqueue_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_5fdb3d66 ON public.recap_processingqueue USING btree (date_modified);


--
-- Name: recap_processingqueue_7a46e69c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_7a46e69c ON public.recap_processingqueue USING btree (court_id);


--
-- Name: recap_processingqueue_af76a535; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_af76a535 ON public.recap_processingqueue USING btree (uploader_id);


--
-- Name: recap_processingqueue_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_c69e55a4 ON public.recap_processingqueue USING btree (date_created);


--
-- Name: recap_processingqueue_court_id_58a50ea026638d4b_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_court_id_58a50ea026638d4b_like ON public.recap_processingqueue USING btree (court_id varchar_pattern_ops);


--
-- Name: recap_processingqueue_pacer_case_id_7602450bbf15145d_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_pacer_case_id_7602450bbf15145d_uniq ON public.recap_processingqueue USING btree (pacer_case_id);


--
-- Name: recap_processingqueue_pacer_doc_id_1a6b8a9884da356a_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_pacer_doc_id_1a6b8a9884da356a_like ON public.recap_processingqueue USING btree (pacer_doc_id varchar_pattern_ops);


--
-- Name: recap_processingqueue_pacer_doc_id_1a6b8a9884da356a_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_pacer_doc_id_1a6b8a9884da356a_uniq ON public.recap_processingqueue USING btree (pacer_doc_id);


--
-- Name: recap_processingqueue_status_526aab9df4d52170_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_processingqueue_status_526aab9df4d52170_uniq ON public.recap_processingqueue USING btree (status);


--
-- Name: recap_rss_rssfeeddata_court_id_8bd4988e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeeddata_court_id_8bd4988e ON public.recap_rss_rssfeeddata USING btree (court_id);


--
-- Name: recap_rss_rssfeeddata_court_id_8bd4988e_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeeddata_court_id_8bd4988e_like ON public.recap_rss_rssfeeddata USING btree (court_id varchar_pattern_ops);


--
-- Name: recap_rss_rssfeeddata_date_created_0b97403f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeeddata_date_created_0b97403f ON public.recap_rss_rssfeeddata USING btree (date_created);


--
-- Name: recap_rss_rssfeeddata_date_modified_cfe95447; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeeddata_date_modified_cfe95447 ON public.recap_rss_rssfeeddata USING btree (date_modified);


--
-- Name: recap_rss_rssfeedstatus_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeedstatus_5fdb3d66 ON public.recap_rss_rssfeedstatus USING btree (date_modified);


--
-- Name: recap_rss_rssfeedstatus_7a46e69c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeedstatus_7a46e69c ON public.recap_rss_rssfeedstatus USING btree (court_id);


--
-- Name: recap_rss_rssfeedstatus_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeedstatus_c69e55a4 ON public.recap_rss_rssfeedstatus USING btree (date_created);


--
-- Name: recap_rss_rssfeedstatus_court_id_41df24dc437f861a_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssfeedstatus_court_id_41df24dc437f861a_like ON public.recap_rss_rssfeedstatus USING btree (court_id varchar_pattern_ops);


--
-- Name: recap_rss_rssitemcache_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssitemcache_c69e55a4 ON public.recap_rss_rssitemcache USING btree (date_created);


--
-- Name: recap_rss_rssitemcache_hash_42acf2127da352d9_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX recap_rss_rssitemcache_hash_42acf2127da352d9_like ON public.recap_rss_rssitemcache USING btree (hash varchar_pattern_ops);


--
-- Name: search_bankruptcyinformation_date_created_60f180b0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_bankruptcyinformation_date_created_60f180b0 ON public.search_bankruptcyinformation USING btree (date_created);


--
-- Name: search_bankruptcyinformation_date_modified_c1b76dd9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_bankruptcyinformation_date_modified_c1b76dd9 ON public.search_bankruptcyinformation USING btree (date_modified);


--
-- Name: search_bankruptcyinformationevent_docket_id_e6ca7d29; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_bankruptcyinformationevent_docket_id_e6ca7d29 ON public.search_bankruptcyinformationevent USING btree (docket_id);


--
-- Name: search_bankruptcyinformationevent_pgh_context_id_5e7bd505; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_bankruptcyinformationevent_pgh_context_id_5e7bd505 ON public.search_bankruptcyinformationevent USING btree (pgh_context_id);


--
-- Name: search_bankruptcyinformationevent_pgh_obj_id_73c1db25; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_bankruptcyinformationevent_pgh_obj_id_73c1db25 ON public.search_bankruptcyinformationevent USING btree (pgh_obj_id);


--
-- Name: search_citation_966557f0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citation_966557f0 ON public.search_citation USING btree (reporter);


--
-- Name: search_citation_a97b1c12; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citation_a97b1c12 ON public.search_citation USING btree (cluster_id);


--
-- Name: search_citation_reporter_1b48f47a0886ffdd_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citation_reporter_1b48f47a0886ffdd_like ON public.search_citation USING btree (reporter text_pattern_ops);


--
-- Name: search_citation_volume_251bc1d270a8abee_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citation_volume_251bc1d270a8abee_idx ON public.search_citation USING btree (volume, reporter);


--
-- Name: search_citation_volume_ae340b5b02e8912_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citation_volume_ae340b5b02e8912_idx ON public.search_citation USING btree (volume, reporter, page);


--
-- Name: search_citationevent_cluster_id_3cc4bdde; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citationevent_cluster_id_3cc4bdde ON public.search_citationevent USING btree (cluster_id);


--
-- Name: search_citationevent_pgh_context_id_a721796b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citationevent_pgh_context_id_a721796b ON public.search_citationevent USING btree (pgh_context_id);


--
-- Name: search_citationevent_pgh_obj_id_74bef0e4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_citationevent_pgh_obj_id_74bef0e4 ON public.search_citationevent USING btree (pgh_obj_id);


--
-- Name: search_claim_claim_number_263236b3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_claim_number_263236b3 ON public.search_claim USING btree (claim_number);


--
-- Name: search_claim_claim_number_263236b3_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_claim_number_263236b3_like ON public.search_claim USING btree (claim_number varchar_pattern_ops);


--
-- Name: search_claim_date_created_8c2e998c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_date_created_8c2e998c ON public.search_claim USING btree (date_created);


--
-- Name: search_claim_date_modified_f38130a2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_date_modified_f38130a2 ON public.search_claim USING btree (date_modified);


--
-- Name: search_claim_docket_id_b37171a9; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_docket_id_b37171a9 ON public.search_claim USING btree (docket_id);


--
-- Name: search_claim_tags_claim_id_2cf554b5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_tags_claim_id_2cf554b5 ON public.search_claim_tags USING btree (claim_id);


--
-- Name: search_claim_tags_tag_id_73b6bd4d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claim_tags_tag_id_73b6bd4d ON public.search_claim_tags USING btree (tag_id);


--
-- Name: search_claimevent_docket_id_b016b91c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimevent_docket_id_b016b91c ON public.search_claimevent USING btree (docket_id);


--
-- Name: search_claimevent_pgh_context_id_421c9863; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimevent_pgh_context_id_421c9863 ON public.search_claimevent USING btree (pgh_context_id);


--
-- Name: search_claimevent_pgh_obj_id_eb8bb005; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimevent_pgh_obj_id_eb8bb005 ON public.search_claimevent USING btree (pgh_obj_id);


--
-- Name: search_claimhistory_claim_id_e130e572; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_claim_id_e130e572 ON public.search_claimhistory USING btree (claim_id);


--
-- Name: search_claimhistory_date_created_586d545e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_date_created_586d545e ON public.search_claimhistory USING btree (date_created);


--
-- Name: search_claimhistory_date_modified_5f6ec339; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_date_modified_5f6ec339 ON public.search_claimhistory USING btree (date_modified);


--
-- Name: search_claimhistory_document_number_6316c155; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_document_number_6316c155 ON public.search_claimhistory USING btree (document_number);


--
-- Name: search_claimhistory_document_number_6316c155_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_document_number_6316c155_like ON public.search_claimhistory USING btree (document_number varchar_pattern_ops);


--
-- Name: search_claimhistory_is_free_on_pacer_81332a2c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistory_is_free_on_pacer_81332a2c ON public.search_claimhistory USING btree (is_free_on_pacer);


--
-- Name: search_claimhistoryevent_claim_id_a256e51f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistoryevent_claim_id_a256e51f ON public.search_claimhistoryevent USING btree (claim_id);


--
-- Name: search_claimhistoryevent_pgh_context_id_fbccd42a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistoryevent_pgh_context_id_fbccd42a ON public.search_claimhistoryevent USING btree (pgh_context_id);


--
-- Name: search_claimhistoryevent_pgh_obj_id_51dc3876; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimhistoryevent_pgh_obj_id_51dc3876 ON public.search_claimhistoryevent USING btree (pgh_obj_id);


--
-- Name: search_claimtagsevent_claim_id_34146335; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimtagsevent_claim_id_34146335 ON public.search_claimtagsevent USING btree (claim_id);


--
-- Name: search_claimtagsevent_pgh_context_id_bb236d3a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimtagsevent_pgh_context_id_bb236d3a ON public.search_claimtagsevent USING btree (pgh_context_id);


--
-- Name: search_claimtagsevent_tag_id_fdeb7331; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_claimtagsevent_tag_id_fdeb7331 ON public.search_claimtagsevent USING btree (tag_id);


--
-- Name: search_court_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_5fdb3d66 ON public.search_court USING btree (date_modified);


--
-- Name: search_court_appeals_to_from_court_id_fb09cc1a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_appeals_to_from_court_id_fb09cc1a ON public.search_court_appeals_to USING btree (from_court_id);


--
-- Name: search_court_appeals_to_from_court_id_fb09cc1a_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_appeals_to_from_court_id_fb09cc1a_like ON public.search_court_appeals_to USING btree (from_court_id varchar_pattern_ops);


--
-- Name: search_court_appeals_to_to_court_id_49ac3d9c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_appeals_to_to_court_id_49ac3d9c ON public.search_court_appeals_to USING btree (to_court_id);


--
-- Name: search_court_appeals_to_to_court_id_49ac3d9c_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_appeals_to_to_court_id_49ac3d9c_like ON public.search_court_appeals_to USING btree (to_court_id varchar_pattern_ops);


--
-- Name: search_court_id_28e1a61bd5ca39cc_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_id_28e1a61bd5ca39cc_like ON public.search_court USING btree (id varchar_pattern_ops);


--
-- Name: search_court_parent_court_id_51ba1d28; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_parent_court_id_51ba1d28 ON public.search_court USING btree (parent_court_id);


--
-- Name: search_court_parent_court_id_51ba1d28_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_court_parent_court_id_51ba1d28_like ON public.search_court USING btree (parent_court_id varchar_pattern_ops);


--
-- Name: search_courtappealstoevent_from_court_id_75784b8f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtappealstoevent_from_court_id_75784b8f ON public.search_courtappealstoevent USING btree (from_court_id);


--
-- Name: search_courtappealstoevent_from_court_id_75784b8f_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtappealstoevent_from_court_id_75784b8f_like ON public.search_courtappealstoevent USING btree (from_court_id varchar_pattern_ops);


--
-- Name: search_courtappealstoevent_pgh_context_id_e65511b3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtappealstoevent_pgh_context_id_e65511b3 ON public.search_courtappealstoevent USING btree (pgh_context_id);


--
-- Name: search_courtappealstoevent_to_court_id_5540ee1b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtappealstoevent_to_court_id_5540ee1b ON public.search_courtappealstoevent USING btree (to_court_id);


--
-- Name: search_courtappealstoevent_to_court_id_5540ee1b_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtappealstoevent_to_court_id_5540ee1b_like ON public.search_courtappealstoevent USING btree (to_court_id varchar_pattern_ops);


--
-- Name: search_courtevent_parent_court_id_342036cc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtevent_parent_court_id_342036cc ON public.search_courtevent USING btree (parent_court_id);


--
-- Name: search_courtevent_parent_court_id_342036cc_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtevent_parent_court_id_342036cc_like ON public.search_courtevent USING btree (parent_court_id varchar_pattern_ops);


--
-- Name: search_courtevent_pgh_context_id_7a93b57e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtevent_pgh_context_id_7a93b57e ON public.search_courtevent USING btree (pgh_context_id);


--
-- Name: search_courtevent_pgh_obj_id_a86c8348; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtevent_pgh_obj_id_a86c8348 ON public.search_courtevent USING btree (pgh_obj_id);


--
-- Name: search_courtevent_pgh_obj_id_a86c8348_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courtevent_pgh_obj_id_a86c8348_like ON public.search_courtevent USING btree (pgh_obj_id varchar_pattern_ops);


--
-- Name: search_courthouse_court_id_6528f572; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouse_court_id_6528f572 ON public.search_courthouse USING btree (court_id);


--
-- Name: search_courthouse_court_id_6528f572_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouse_court_id_6528f572_like ON public.search_courthouse USING btree (court_id varchar_pattern_ops);


--
-- Name: search_courthouseevent_court_id_ecdd5b8a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouseevent_court_id_ecdd5b8a ON public.search_courthouseevent USING btree (court_id);


--
-- Name: search_courthouseevent_court_id_ecdd5b8a_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouseevent_court_id_ecdd5b8a_like ON public.search_courthouseevent USING btree (court_id varchar_pattern_ops);


--
-- Name: search_courthouseevent_pgh_context_id_affccfe3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouseevent_pgh_context_id_affccfe3 ON public.search_courthouseevent USING btree (pgh_context_id);


--
-- Name: search_courthouseevent_pgh_obj_id_2bdd6824; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_courthouseevent_pgh_obj_id_2bdd6824 ON public.search_courthouseevent USING btree (pgh_obj_id);


--
-- Name: search_dock_court_i_a043ae_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_dock_court_i_a043ae_idx ON public.search_docket USING btree (court_id, id);


--
-- Name: search_docket_02c1725c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_02c1725c ON public.search_docket USING btree (assigned_to_id);


--
-- Name: search_docket_0b869b2f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_0b869b2f ON public.search_docket USING btree (date_blocked);


--
-- Name: search_docket_34894a03; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_34894a03 ON public.search_docket USING btree (pacer_case_id);


--
-- Name: search_docket_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_5fdb3d66 ON public.search_docket USING btree (date_modified);


--
-- Name: search_docket_64cdae4d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_64cdae4d ON public.search_docket USING btree (referred_to_id);


--
-- Name: search_docket_695b63bb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_695b63bb ON public.search_docket USING btree (appeal_from_id);


--
-- Name: search_docket_6c91ba55; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_6c91ba55 ON public.search_docket USING btree (docket_number_core);


--
-- Name: search_docket_7a46e69c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_7a46e69c ON public.search_docket USING btree (court_id);


--
-- Name: search_docket_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_c69e55a4 ON public.search_docket USING btree (date_created);


--
-- Name: search_docket_court_id_2d2438b2594e74ba_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_court_id_2d2438b2594e74ba_like ON public.search_docket USING btree (court_id varchar_pattern_ops);


--
-- Name: search_docket_docket_number_core_713b7b04e01f11d7_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_docket_number_core_713b7b04e01f11d7_like ON public.search_docket USING btree (docket_number_core varchar_pattern_ops);


--
-- Name: search_docket_panel_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_panel_1427d4ab ON public.search_docket_panel USING btree (docket_id);


--
-- Name: search_docket_panel_a8452ca7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_panel_a8452ca7 ON public.search_docket_panel USING btree (person_id);


--
-- Name: search_docket_parent_docket_id_1a514426; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_parent_docket_id_1a514426 ON public.search_docket USING btree (parent_docket_id);


--
-- Name: search_docket_tags_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_tags_1427d4ab ON public.search_docket_tags USING btree (docket_id);


--
-- Name: search_docket_tags_76f094bc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docket_tags_76f094bc ON public.search_docket_tags USING btree (tag_id);


--
-- Name: search_docketentry_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_1427d4ab ON public.search_docketentry USING btree (docket_id);


--
-- Name: search_docketentry_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_5fdb3d66 ON public.search_docketentry USING btree (date_modified);


--
-- Name: search_docketentry_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_c69e55a4 ON public.search_docketentry USING btree (date_created);


--
-- Name: search_docketentry_recap_sequence_number_1c82e51988e2d89f_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_recap_sequence_number_1c82e51988e2d89f_idx ON public.search_docketentry USING btree (recap_sequence_number, entry_number);


--
-- Name: search_docketentry_tags_76f094bc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_tags_76f094bc ON public.search_docketentry_tags USING btree (tag_id);


--
-- Name: search_docketentry_tags_b02d089e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentry_tags_b02d089e ON public.search_docketentry_tags USING btree (docketentry_id);


--
-- Name: search_docketentryevent_docket_id_469ad4c0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentryevent_docket_id_469ad4c0 ON public.search_docketentryevent USING btree (docket_id);


--
-- Name: search_docketentryevent_pgh_context_id_1bd9c36d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentryevent_pgh_context_id_1bd9c36d ON public.search_docketentryevent USING btree (pgh_context_id);


--
-- Name: search_docketentryevent_pgh_obj_id_584ac554; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentryevent_pgh_obj_id_584ac554 ON public.search_docketentryevent USING btree (pgh_obj_id);


--
-- Name: search_docketentrytagsevent_docketentry_id_1aa64197; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentrytagsevent_docketentry_id_1aa64197 ON public.search_docketentrytagsevent USING btree (docketentry_id);


--
-- Name: search_docketentrytagsevent_pgh_context_id_f91c4367; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentrytagsevent_pgh_context_id_f91c4367 ON public.search_docketentrytagsevent USING btree (pgh_context_id);


--
-- Name: search_docketentrytagsevent_tag_id_9d769fa5; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketentrytagsevent_tag_id_9d769fa5 ON public.search_docketentrytagsevent USING btree (tag_id);


--
-- Name: search_docketevent_appeal_from_id_388367c7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_appeal_from_id_388367c7 ON public.search_docketevent USING btree (appeal_from_id);


--
-- Name: search_docketevent_appeal_from_id_388367c7_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_appeal_from_id_388367c7_like ON public.search_docketevent USING btree (appeal_from_id varchar_pattern_ops);


--
-- Name: search_docketevent_assigned_to_id_13bac477; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_assigned_to_id_13bac477 ON public.search_docketevent USING btree (assigned_to_id);


--
-- Name: search_docketevent_court_id_c6baeb82; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_court_id_c6baeb82 ON public.search_docketevent USING btree (court_id);


--
-- Name: search_docketevent_court_id_c6baeb82_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_court_id_c6baeb82_like ON public.search_docketevent USING btree (court_id varchar_pattern_ops);


--
-- Name: search_docketevent_idb_data_id_62179a0f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_idb_data_id_62179a0f ON public.search_docketevent USING btree (idb_data_id);


--
-- Name: search_docketevent_originating_court_information_id_47acc418; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_originating_court_information_id_47acc418 ON public.search_docketevent USING btree (originating_court_information_id);


--
-- Name: search_docketevent_parent_docket_id_c7c9c9ad; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_parent_docket_id_c7c9c9ad ON public.search_docketevent USING btree (parent_docket_id);


--
-- Name: search_docketevent_pgh_context_id_72300038; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_pgh_context_id_72300038 ON public.search_docketevent USING btree (pgh_context_id);


--
-- Name: search_docketevent_pgh_obj_id_5d06013e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_pgh_obj_id_5d06013e ON public.search_docketevent USING btree (pgh_obj_id);


--
-- Name: search_docketevent_referred_to_id_ba58a272; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketevent_referred_to_id_ba58a272 ON public.search_docketevent USING btree (referred_to_id);


--
-- Name: search_docketpanelevent_docket_id_1a9e206c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketpanelevent_docket_id_1a9e206c ON public.search_docketpanelevent USING btree (docket_id);


--
-- Name: search_docketpanelevent_person_id_97094b3d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketpanelevent_person_id_97094b3d ON public.search_docketpanelevent USING btree (person_id);


--
-- Name: search_docketpanelevent_pgh_context_id_03019aa7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_docketpanelevent_pgh_context_id_03019aa7 ON public.search_docketpanelevent USING btree (pgh_context_id);


--
-- Name: search_dockettagsevent_docket_id_b1874f82; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_dockettagsevent_docket_id_b1874f82 ON public.search_dockettagsevent USING btree (docket_id);


--
-- Name: search_dockettagsevent_pgh_context_id_69b62450; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_dockettagsevent_pgh_context_id_69b62450 ON public.search_dockettagsevent USING btree (pgh_context_id);


--
-- Name: search_dockettagsevent_tag_id_728990f4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_dockettagsevent_tag_id_728990f4 ON public.search_dockettagsevent USING btree (tag_id);


--
-- Name: search_opin_depth_3307bd_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opin_depth_3307bd_idx ON public.search_opinionscitedbyrecapdocument USING btree (depth);


--
-- Name: search_opinion_1cbcfc0f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_1cbcfc0f ON public.search_opinion USING btree (download_url);


--
-- Name: search_opinion_4f331e2f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_4f331e2f ON public.search_opinion USING btree (author_id);


--
-- Name: search_opinion_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_5fdb3d66 ON public.search_opinion USING btree (date_modified);


--
-- Name: search_opinion_71485e76; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_71485e76 ON public.search_opinion USING btree (local_path);


--
-- Name: search_opinion_74a89174; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_74a89174 ON public.search_opinion USING btree (sha1);


--
-- Name: search_opinion_a97b1c12; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_a97b1c12 ON public.search_opinion USING btree (cluster_id);


--
-- Name: search_opinion_bded6737; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_bded6737 ON public.search_opinion USING btree (extracted_by_ocr);


--
-- Name: search_opinion_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_c69e55a4 ON public.search_opinion USING btree (date_created);


--
-- Name: search_opinion_download_url_3b11b165f23bc568_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_download_url_3b11b165f23bc568_like ON public.search_opinion USING btree (download_url varchar_pattern_ops);


--
-- Name: search_opinion_joined_by_8a09c46f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_joined_by_8a09c46f ON public.search_opinion_joined_by USING btree (opinion_id);


--
-- Name: search_opinion_joined_by_e7c5d788; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_joined_by_e7c5d788 ON public.search_opinion_joined_by USING btree (person_id);


--
-- Name: search_opinion_local_path_63290b39b28ef927_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_local_path_63290b39b28ef927_like ON public.search_opinion USING btree (local_path varchar_pattern_ops);


--
-- Name: search_opinion_sha1_5887dd5d3475ad17_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinion_sha1_5887dd5d3475ad17_like ON public.search_opinion USING btree (sha1 varchar_pattern_ops);


--
-- Name: search_opinioncluster_0b869b2f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_0b869b2f ON public.search_opinioncluster USING btree (date_blocked);


--
-- Name: search_opinioncluster_1427d4ab; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_1427d4ab ON public.search_opinioncluster USING btree (docket_id);


--
-- Name: search_opinioncluster_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_5fdb3d66 ON public.search_opinioncluster USING btree (date_modified);


--
-- Name: search_opinioncluster_61326117; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_61326117 ON public.search_opinioncluster USING btree (blocked);


--
-- Name: search_opinioncluster_98cc5f59; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_98cc5f59 ON public.search_opinioncluster USING btree (date_filed);


--
-- Name: search_opinioncluster_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_c69e55a4 ON public.search_opinioncluster USING btree (date_created);


--
-- Name: search_opinioncluster_d91c83eb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_d91c83eb ON public.search_opinioncluster USING btree (citation_count);


--
-- Name: search_opinioncluster_e0fd3ccf; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_e0fd3ccf ON public.search_opinioncluster USING btree (scdb_id);


--
-- Name: search_opinioncluster_f796c05b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_f796c05b ON public.search_opinioncluster USING btree (precedential_status);


--
-- Name: search_opinioncluster_filepath_json_harvard_4b8057d0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_filepath_json_harvard_4b8057d0 ON public.search_opinioncluster USING btree (filepath_json_harvard);


--
-- Name: search_opinioncluster_filepath_json_harvard_4b8057d0_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_filepath_json_harvard_4b8057d0_like ON public.search_opinioncluster USING btree (filepath_json_harvard varchar_pattern_ops);


--
-- Name: search_opinioncluster_non_participating_judges_49bb60ae; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_non_participating_judges_49bb60ae ON public.search_opinioncluster_non_participating_judges USING btree (opinioncluster_id);


--
-- Name: search_opinioncluster_non_participating_judges_e7c5d788; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_non_participating_judges_e7c5d788 ON public.search_opinioncluster_non_participating_judges USING btree (person_id);


--
-- Name: search_opinioncluster_panel_49bb60ae; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_panel_49bb60ae ON public.search_opinioncluster_panel USING btree (opinioncluster_id);


--
-- Name: search_opinioncluster_panel_e7c5d788; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_panel_e7c5d788 ON public.search_opinioncluster_panel USING btree (person_id);


--
-- Name: search_opinioncluster_precedential_status_2bdec6e2dedba28b_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_precedential_status_2bdec6e2dedba28b_like ON public.search_opinioncluster USING btree (precedential_status varchar_pattern_ops);


--
-- Name: search_opinioncluster_supreme_court_db_id_4297061ed6ba336_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinioncluster_supreme_court_db_id_4297061ed6ba336_like ON public.search_opinioncluster USING btree (scdb_id varchar_pattern_ops);


--
-- Name: search_opinionclusterevent_docket_id_165932da; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterevent_docket_id_165932da ON public.search_opinionclusterevent USING btree (docket_id);


--
-- Name: search_opinionclusterevent_pgh_context_id_273003da; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterevent_pgh_context_id_273003da ON public.search_opinionclusterevent USING btree (pgh_context_id);


--
-- Name: search_opinionclusterevent_pgh_obj_id_f1ea380d; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterevent_pgh_obj_id_f1ea380d ON public.search_opinionclusterevent USING btree (pgh_obj_id);


--
-- Name: search_opinionclusternonpa_opinioncluster_id_cc505710; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusternonpa_opinioncluster_id_cc505710 ON public.search_opinionclusternonparticipatingjudgesevent USING btree (opinioncluster_id);


--
-- Name: search_opinionclusternonpa_person_id_7bf4f773; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusternonpa_person_id_7bf4f773 ON public.search_opinionclusternonparticipatingjudgesevent USING btree (person_id);


--
-- Name: search_opinionclusternonpa_pgh_context_id_aef74bea; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusternonpa_pgh_context_id_aef74bea ON public.search_opinionclusternonparticipatingjudgesevent USING btree (pgh_context_id);


--
-- Name: search_opinionclusterpanelevent_opinioncluster_id_7128c9e4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterpanelevent_opinioncluster_id_7128c9e4 ON public.search_opinionclusterpanelevent USING btree (opinioncluster_id);


--
-- Name: search_opinionclusterpanelevent_person_id_b1c6a4a7; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterpanelevent_person_id_b1c6a4a7 ON public.search_opinionclusterpanelevent USING btree (person_id);


--
-- Name: search_opinionclusterpanelevent_pgh_context_id_8dcb8078; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionclusterpanelevent_pgh_context_id_8dcb8078 ON public.search_opinionclusterpanelevent USING btree (pgh_context_id);


--
-- Name: search_opinionevent_author_id_43b0c67a; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionevent_author_id_43b0c67a ON public.search_opinionevent USING btree (author_id);


--
-- Name: search_opinionevent_cluster_id_1205465b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionevent_cluster_id_1205465b ON public.search_opinionevent USING btree (cluster_id);


--
-- Name: search_opinionevent_pgh_context_id_723082e0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionevent_pgh_context_id_723082e0 ON public.search_opinionevent USING btree (pgh_context_id);


--
-- Name: search_opinionevent_pgh_obj_id_63a2bc5f; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionevent_pgh_obj_id_63a2bc5f ON public.search_opinionevent USING btree (pgh_obj_id);


--
-- Name: search_opinionjoinedbyevent_opinion_id_9271b281; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionjoinedbyevent_opinion_id_9271b281 ON public.search_opinionjoinedbyevent USING btree (opinion_id);


--
-- Name: search_opinionjoinedbyevent_person_id_dffa9dcb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionjoinedbyevent_person_id_dffa9dcb ON public.search_opinionjoinedbyevent USING btree (person_id);


--
-- Name: search_opinionjoinedbyevent_pgh_context_id_48acc9ad; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionjoinedbyevent_pgh_context_id_48acc9ad ON public.search_opinionjoinedbyevent USING btree (pgh_context_id);


--
-- Name: search_opinionscited_5b8b69a0; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionscited_5b8b69a0 ON public.search_opinionscited USING btree (citing_opinion_id);


--
-- Name: search_opinionscited_740050e6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionscited_740050e6 ON public.search_opinionscited USING btree (cited_opinion_id);


--
-- Name: search_opinionscited_depth_46bacaef; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionscited_depth_46bacaef ON public.search_opinionscited USING btree (depth);


--
-- Name: search_opinionscitedbyrecapdocument_cited_opinion_id_5f0347bb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionscitedbyrecapdocument_cited_opinion_id_5f0347bb ON public.search_opinionscitedbyrecapdocument USING btree (cited_opinion_id);


--
-- Name: search_opinionscitedbyrecapdocument_citing_document_id_c64b751b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_opinionscitedbyrecapdocument_citing_document_id_c64b751b ON public.search_opinionscitedbyrecapdocument USING btree (citing_document_id);


--
-- Name: search_originatingcourtinf_ordering_judge_id_5aa931cb; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinf_ordering_judge_id_5aa931cb ON public.search_originatingcourtinformationevent USING btree (ordering_judge_id);


--
-- Name: search_originatingcourtinformation_02c1725c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformation_02c1725c ON public.search_originatingcourtinformation USING btree (assigned_to_id);


--
-- Name: search_originatingcourtinformation_09a6c128; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformation_09a6c128 ON public.search_originatingcourtinformation USING btree (ordering_judge_id);


--
-- Name: search_originatingcourtinformation_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformation_5fdb3d66 ON public.search_originatingcourtinformation USING btree (date_modified);


--
-- Name: search_originatingcourtinformation_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformation_c69e55a4 ON public.search_originatingcourtinformation USING btree (date_created);


--
-- Name: search_originatingcourtinformationevent_assigned_to_id_fcce9094; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformationevent_assigned_to_id_fcce9094 ON public.search_originatingcourtinformationevent USING btree (assigned_to_id);


--
-- Name: search_originatingcourtinformationevent_pgh_context_id_d8ffc4c8; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformationevent_pgh_context_id_d8ffc4c8 ON public.search_originatingcourtinformationevent USING btree (pgh_context_id);


--
-- Name: search_originatingcourtinformationevent_pgh_obj_id_32490a9c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_originatingcourtinformationevent_pgh_obj_id_32490a9c ON public.search_originatingcourtinformationevent USING btree (pgh_obj_id);


--
-- Name: search_pare_score_16f118_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_pare_score_16f118_idx ON public.search_parentheticalgroup USING btree (score);


--
-- Name: search_parenthetical_described_opinion_id_ddd408db; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parenthetical_described_opinion_id_ddd408db ON public.search_parenthetical USING btree (described_opinion_id);


--
-- Name: search_parenthetical_describing_opinion_id_07864494; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parenthetical_describing_opinion_id_07864494 ON public.search_parenthetical USING btree (describing_opinion_id);


--
-- Name: search_parenthetical_group_id_00a7def3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parenthetical_group_id_00a7def3 ON public.search_parenthetical USING btree (group_id);


--
-- Name: search_parenthetical_score_cab0b2a1; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parenthetical_score_cab0b2a1 ON public.search_parenthetical USING btree (score);


--
-- Name: search_parentheticalgroup_opinion_id_fd6bb935; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parentheticalgroup_opinion_id_fd6bb935 ON public.search_parentheticalgroup USING btree (opinion_id);


--
-- Name: search_parentheticalgroup_representative_id_00e5a857; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_parentheticalgroup_representative_id_00e5a857 ON public.search_parentheticalgroup USING btree (representative_id);


--
-- Name: search_recapdocument_1dec38a6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_1dec38a6 ON public.search_recapdocument USING btree (is_free_on_pacer);


--
-- Name: search_recapdocument_40d913b2; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_40d913b2 ON public.search_recapdocument USING btree (docket_entry_id);


--
-- Name: search_recapdocument_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_5fdb3d66 ON public.search_recapdocument USING btree (date_modified);


--
-- Name: search_recapdocument_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_c69e55a4 ON public.search_recapdocument USING btree (date_created);


--
-- Name: search_recapdocument_document_number_6f825e81ddd11fde_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_document_number_6f825e81ddd11fde_uniq ON public.search_recapdocument USING btree (document_number);


--
-- Name: search_recapdocument_document_type_303cccac79571217_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_document_type_303cccac79571217_idx ON public.search_recapdocument USING btree (document_type, document_number, attachment_number);


--
-- Name: search_recapdocument_filepath_local_7dc6b0e53ccf753_uniq; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_filepath_local_7dc6b0e53ccf753_uniq ON public.search_recapdocument USING btree (filepath_local);


--
-- Name: search_recapdocument_pacer_doc_id_12ec9c122839e6aa_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_pacer_doc_id_12ec9c122839e6aa_like ON public.search_recapdocument USING btree (pacer_doc_id varchar_pattern_ops);


--
-- Name: search_recapdocument_tags_76f094bc; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_tags_76f094bc ON public.search_recapdocument_tags USING btree (tag_id);


--
-- Name: search_recapdocument_tags_fae26ff6; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocument_tags_fae26ff6 ON public.search_recapdocument_tags USING btree (recapdocument_id);


--
-- Name: search_recapdocumentevent_docket_entry_id_055ee57b; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumentevent_docket_entry_id_055ee57b ON public.search_recapdocumentevent USING btree (docket_entry_id);


--
-- Name: search_recapdocumentevent_pgh_context_id_37bf47c3; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumentevent_pgh_context_id_37bf47c3 ON public.search_recapdocumentevent USING btree (pgh_context_id);


--
-- Name: search_recapdocumentevent_pgh_obj_id_aa9c8d6e; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumentevent_pgh_obj_id_aa9c8d6e ON public.search_recapdocumentevent USING btree (pgh_obj_id);


--
-- Name: search_recapdocumenttagsevent_pgh_context_id_ff9be284; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumenttagsevent_pgh_context_id_ff9be284 ON public.search_recapdocumenttagsevent USING btree (pgh_context_id);


--
-- Name: search_recapdocumenttagsevent_recapdocument_id_c6f0a858; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumenttagsevent_recapdocument_id_c6f0a858 ON public.search_recapdocumenttagsevent USING btree (recapdocument_id);


--
-- Name: search_recapdocumenttagsevent_tag_id_9fa96f02; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_recapdocumenttagsevent_tag_id_9fa96f02 ON public.search_recapdocumenttagsevent USING btree (tag_id);


--
-- Name: search_sear_date_cr_c5fff9_idx; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_sear_date_cr_c5fff9_idx ON public.search_searchquery USING btree (date_created);


--
-- Name: search_searchquery_user_id_8918791c; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_searchquery_user_id_8918791c ON public.search_searchquery USING btree (user_id);


--
-- Name: search_tag_5fdb3d66; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_tag_5fdb3d66 ON public.search_tag USING btree (date_modified);


--
-- Name: search_tag_c69e55a4; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_tag_c69e55a4 ON public.search_tag USING btree (date_created);


--
-- Name: search_tag_name_30c16b352387258b_like; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_tag_name_30c16b352387258b_like ON public.search_tag USING btree (name varchar_pattern_ops);


--
-- Name: search_tagevent_pgh_context_id_03f699de; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_tagevent_pgh_context_id_03f699de ON public.search_tagevent USING btree (pgh_context_id);


--
-- Name: search_tagevent_pgh_obj_id_af8c9817; Type: INDEX; Schema: public; Owner: django
--

CREATE INDEX search_tagevent_pgh_obj_id_af8c9817 ON public.search_tagevent USING btree (pgh_obj_id);


--
-- Name: disclosures_debt pgtrigger_delete_delete_005d4; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_005d4 AFTER DELETE ON public.disclosures_debt FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_005d4();


--
-- Name: TRIGGER pgtrigger_delete_delete_005d4 ON disclosures_debt; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_005d4 ON public.disclosures_debt IS '7facaca4a69b1d4b9db3270ea4e2d2b77164b8a7';


--
-- Name: search_court_appeals_to pgtrigger_delete_delete_12060; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_12060 AFTER DELETE ON public.search_court_appeals_to FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_12060();


--
-- Name: TRIGGER pgtrigger_delete_delete_12060 ON search_court_appeals_to; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_12060 ON public.search_court_appeals_to IS 'bf003876e74bc51f807761d977b845b3c3e7cf7e';


--
-- Name: people_db_retentionevent pgtrigger_delete_delete_15a5d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_15a5d AFTER DELETE ON public.people_db_retentionevent FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_15a5d();


--
-- Name: TRIGGER pgtrigger_delete_delete_15a5d ON people_db_retentionevent; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_15a5d ON public.people_db_retentionevent IS 'f2302aa32ca9863dd528bed3ec6d041c34fad306';


--
-- Name: audio_audio pgtrigger_delete_delete_15ad9; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_15ad9 AFTER DELETE ON public.audio_audio FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_15ad9();


--
-- Name: TRIGGER pgtrigger_delete_delete_15ad9 ON audio_audio; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_15ad9 ON public.audio_audio IS '3cd590791fb720db3c7ec3d0a8ecd7de9f7d9e84';


--
-- Name: search_claim_tags pgtrigger_delete_delete_1a74e; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_1a74e AFTER DELETE ON public.search_claim_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_1a74e();


--
-- Name: TRIGGER pgtrigger_delete_delete_1a74e ON search_claim_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_1a74e ON public.search_claim_tags IS '3f208ceb5c460fc3114b02ecd39c4d32d2796d81';


--
-- Name: search_docket_tags pgtrigger_delete_delete_226b5; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_226b5 AFTER DELETE ON public.search_docket_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_226b5();


--
-- Name: TRIGGER pgtrigger_delete_delete_226b5 ON search_docket_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_226b5 ON public.search_docket_tags IS 'cda059d1986a3d69571e71dd3b5f5c8f6f938e95';


--
-- Name: search_opinion_joined_by pgtrigger_delete_delete_243ae; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_243ae AFTER DELETE ON public.search_opinion_joined_by FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_243ae();


--
-- Name: TRIGGER pgtrigger_delete_delete_243ae ON search_opinion_joined_by; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_243ae ON public.search_opinion_joined_by IS '137440463b89d04d25fd8304c7f40b76896a99b1';


--
-- Name: search_tag pgtrigger_delete_delete_24781; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_24781 AFTER DELETE ON public.search_tag FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_24781();


--
-- Name: TRIGGER pgtrigger_delete_delete_24781 ON search_tag; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_24781 ON public.search_tag IS '018ef845ef76126676f8b3d1c2c0a0fe0f660bbe';


--
-- Name: search_recapdocument pgtrigger_delete_delete_28a84; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_28a84 AFTER DELETE ON public.search_recapdocument FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_28a84();


--
-- Name: TRIGGER pgtrigger_delete_delete_28a84 ON search_recapdocument; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_28a84 ON public.search_recapdocument IS '71e2e1d3b8eacab1d2386c0041629d81e3d99574';


--
-- Name: search_opinioncluster_non_participating_judges pgtrigger_delete_delete_2bcae; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_2bcae AFTER DELETE ON public.search_opinioncluster_non_participating_judges FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_2bcae();


--
-- Name: TRIGGER pgtrigger_delete_delete_2bcae ON search_opinioncluster_non_participating_judges; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_2bcae ON public.search_opinioncluster_non_participating_judges IS 'c0a73b419c24b7969bd3dd589ffbb760db1e0e1d';


--
-- Name: disclosures_reimbursement pgtrigger_delete_delete_320a7; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_320a7 AFTER DELETE ON public.disclosures_reimbursement FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_320a7();


--
-- Name: TRIGGER pgtrigger_delete_delete_320a7 ON disclosures_reimbursement; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_320a7 ON public.disclosures_reimbursement IS 'c8e13b897cad3c4af279b0b325e261997d44b95c';


--
-- Name: people_db_abarating pgtrigger_delete_delete_328c3; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_328c3 AFTER DELETE ON public.people_db_abarating FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_328c3();


--
-- Name: TRIGGER pgtrigger_delete_delete_328c3 ON people_db_abarating; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_328c3 ON public.people_db_abarating IS '8ac8cb3fbccac85e01f87fea2a4fea9725b3211c';


--
-- Name: people_db_person pgtrigger_delete_delete_3bdd3; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_3bdd3 AFTER DELETE ON public.people_db_person FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_3bdd3();


--
-- Name: TRIGGER pgtrigger_delete_delete_3bdd3 ON people_db_person; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_3bdd3 ON public.people_db_person IS '79a19882c54b7b71eb73e97da70437d8960e422c';


--
-- Name: people_db_person_race pgtrigger_delete_delete_53db8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_53db8 AFTER DELETE ON public.people_db_person_race FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_53db8();


--
-- Name: TRIGGER pgtrigger_delete_delete_53db8 ON people_db_person_race; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_53db8 ON public.people_db_person_race IS '3679cfa4190e443aa2d99b5dfd2cf459d2770260';


--
-- Name: disclosures_noninvestmentincome pgtrigger_delete_delete_5412a; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_5412a AFTER DELETE ON public.disclosures_noninvestmentincome FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_5412a();


--
-- Name: TRIGGER pgtrigger_delete_delete_5412a ON disclosures_noninvestmentincome; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_5412a ON public.disclosures_noninvestmentincome IS '36799dd5f68cfcc0c7fba63c771abf864ed51909';


--
-- Name: search_citation pgtrigger_delete_delete_58ea6; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_58ea6 AFTER DELETE ON public.search_citation FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_58ea6();


--
-- Name: TRIGGER pgtrigger_delete_delete_58ea6 ON search_citation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_58ea6 ON public.search_citation IS 'cbd4a7bb7d831a74b557c16ec08c05ef1639c903';


--
-- Name: search_opinioncluster_panel pgtrigger_delete_delete_5a6ce; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_5a6ce AFTER DELETE ON public.search_opinioncluster_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_5a6ce();


--
-- Name: TRIGGER pgtrigger_delete_delete_5a6ce ON search_opinioncluster_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_5a6ce ON public.search_opinioncluster_panel IS '8f1743d545986edd98887671ade6ea7a7ddf1828';


--
-- Name: disclosures_agreement pgtrigger_delete_delete_5cad8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_5cad8 AFTER DELETE ON public.disclosures_agreement FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_5cad8();


--
-- Name: TRIGGER pgtrigger_delete_delete_5cad8 ON disclosures_agreement; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_5cad8 ON public.disclosures_agreement IS '162fef86f5afd641a11e73fc90ed0f3d0dec26a0';


--
-- Name: search_opinion pgtrigger_delete_delete_613d8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_613d8 AFTER DELETE ON public.search_opinion FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_613d8();


--
-- Name: TRIGGER pgtrigger_delete_delete_613d8 ON search_opinion; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_613d8 ON public.search_opinion IS '978d3c48a0e531c7d74088c973fd529230b95678';


--
-- Name: search_bankruptcyinformation pgtrigger_delete_delete_62e3d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_62e3d AFTER DELETE ON public.search_bankruptcyinformation FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_62e3d();


--
-- Name: TRIGGER pgtrigger_delete_delete_62e3d ON search_bankruptcyinformation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_62e3d ON public.search_bankruptcyinformation IS '06cd921a038bd165830cb8dc7f1897dd8bf46c22';


--
-- Name: disclosures_position pgtrigger_delete_delete_67b7d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_67b7d AFTER DELETE ON public.disclosures_position FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_67b7d();


--
-- Name: TRIGGER pgtrigger_delete_delete_67b7d ON disclosures_position; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_67b7d ON public.disclosures_position IS '8f2cb19e5f2c3ec415c20ad03eebcf45331624c4';


--
-- Name: search_courthouse pgtrigger_delete_delete_6f488; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_6f488 AFTER DELETE ON public.search_courthouse FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_6f488();


--
-- Name: TRIGGER pgtrigger_delete_delete_6f488 ON search_courthouse; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_6f488 ON public.search_courthouse IS '4bc49bfad632207bc23d2920130608dca8708df0';


--
-- Name: search_docket pgtrigger_delete_delete_7303c; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_7303c AFTER DELETE ON public.search_docket FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_7303c();


--
-- Name: TRIGGER pgtrigger_delete_delete_7303c ON search_docket; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_7303c ON public.search_docket IS '120d2ce6f53809c06f51a3a2793caa88ae883fdf';


--
-- Name: people_db_school pgtrigger_delete_delete_7b0f9; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_7b0f9 AFTER DELETE ON public.people_db_school FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_7b0f9();


--
-- Name: TRIGGER pgtrigger_delete_delete_7b0f9 ON people_db_school; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_7b0f9 ON public.people_db_school IS '5ef3e418face8d0ff04fb5a47c2e23cb84729fce';


--
-- Name: people_db_politicalaffiliation pgtrigger_delete_delete_868cc; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_868cc AFTER DELETE ON public.people_db_politicalaffiliation FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_868cc();


--
-- Name: TRIGGER pgtrigger_delete_delete_868cc ON people_db_politicalaffiliation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_868cc ON public.people_db_politicalaffiliation IS '7b68ad289297e13a8a0399225f5feb659d4a3702';


--
-- Name: search_docketentry_tags pgtrigger_delete_delete_88ce1; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_88ce1 AFTER DELETE ON public.search_docketentry_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_88ce1();


--
-- Name: TRIGGER pgtrigger_delete_delete_88ce1 ON search_docketentry_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_88ce1 ON public.search_docketentry_tags IS 'ed4129077b3c45887dac263537ecd4a450f88ab0';


--
-- Name: search_claimhistory pgtrigger_delete_delete_8a93d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_8a93d AFTER DELETE ON public.search_claimhistory FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_8a93d();


--
-- Name: TRIGGER pgtrigger_delete_delete_8a93d ON search_claimhistory; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_8a93d ON public.search_claimhistory IS '3a9be05fd4c10adffd72d02db61564a47519076f';


--
-- Name: search_docketentry pgtrigger_delete_delete_9164f; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_9164f AFTER DELETE ON public.search_docketentry FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_9164f();


--
-- Name: TRIGGER pgtrigger_delete_delete_9164f ON search_docketentry; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_9164f ON public.search_docketentry IS 'd646f9ed3424d3546c8f737e0e073709738c27e2';


--
-- Name: people_db_source pgtrigger_delete_delete_9d499; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_9d499 AFTER DELETE ON public.people_db_source FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_9d499();


--
-- Name: TRIGGER pgtrigger_delete_delete_9d499 ON people_db_source; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_9d499 ON public.people_db_source IS '017c01d3ebc53bbfd806f6eb3dcc046457fff8ce';


--
-- Name: search_opinioncluster pgtrigger_delete_delete_a8516; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_a8516 AFTER DELETE ON public.search_opinioncluster FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_a8516();


--
-- Name: TRIGGER pgtrigger_delete_delete_a8516 ON search_opinioncluster; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_a8516 ON public.search_opinioncluster IS '16c973181b7fa4c3c77e7b854998db6763fab7bd';


--
-- Name: disclosures_investment pgtrigger_delete_delete_b7d5e; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_b7d5e AFTER DELETE ON public.disclosures_investment FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_b7d5e();


--
-- Name: TRIGGER pgtrigger_delete_delete_b7d5e ON disclosures_investment; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_b7d5e ON public.disclosures_investment IS 'a3206b4d1248f3b8ea4789cd2529e6e302af7bf4';


--
-- Name: search_claim pgtrigger_delete_delete_c63fd; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_c63fd AFTER DELETE ON public.search_claim FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_c63fd();


--
-- Name: TRIGGER pgtrigger_delete_delete_c63fd ON search_claim; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_c63fd ON public.search_claim IS '2c7d9f133be0980742a0bda76d49cde2328abb02';


--
-- Name: disclosures_gift pgtrigger_delete_delete_c90b0; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_c90b0 AFTER DELETE ON public.disclosures_gift FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_c90b0();


--
-- Name: TRIGGER pgtrigger_delete_delete_c90b0 ON disclosures_gift; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_c90b0 ON public.disclosures_gift IS '1b97437eaa7457ad251acec353f9e1fdd4922bca';


--
-- Name: audio_audio_panel pgtrigger_delete_delete_cae12; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_cae12 AFTER DELETE ON public.audio_audio_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_cae12();


--
-- Name: TRIGGER pgtrigger_delete_delete_cae12 ON audio_audio_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_cae12 ON public.audio_audio_panel IS '61df118b866f8dbcf1c9b4f7d90281c98aeaec4f';


--
-- Name: search_court pgtrigger_delete_delete_cf358; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_cf358 AFTER DELETE ON public.search_court FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_cf358();


--
-- Name: TRIGGER pgtrigger_delete_delete_cf358 ON search_court; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_cf358 ON public.search_court IS '8d318ab7d986170c8c3fa2abe688c86f87dae40f';


--
-- Name: people_db_position pgtrigger_delete_delete_d9c2f; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_d9c2f AFTER DELETE ON public.people_db_position FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_d9c2f();


--
-- Name: TRIGGER pgtrigger_delete_delete_d9c2f ON people_db_position; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_d9c2f ON public.people_db_position IS 'f6be21514efe680ca6c1bce2f82b091ab0733db7';


--
-- Name: people_db_race pgtrigger_delete_delete_e1249; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_e1249 AFTER DELETE ON public.people_db_race FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_e1249();


--
-- Name: TRIGGER pgtrigger_delete_delete_e1249 ON people_db_race; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_e1249 ON public.people_db_race IS '7e8b396098110fbaf41140d5ea77c0513676ac2f';


--
-- Name: disclosures_spouseincome pgtrigger_delete_delete_e4751; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_e4751 AFTER DELETE ON public.disclosures_spouseincome FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_e4751();


--
-- Name: TRIGGER pgtrigger_delete_delete_e4751 ON disclosures_spouseincome; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_e4751 ON public.disclosures_spouseincome IS '6aad717aa62be736e0e645b2bf0529e0407f9c26';


--
-- Name: people_db_education pgtrigger_delete_delete_e47b7; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_e47b7 AFTER DELETE ON public.people_db_education FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_e47b7();


--
-- Name: TRIGGER pgtrigger_delete_delete_e47b7 ON people_db_education; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_e47b7 ON public.people_db_education IS '383713f24709c2a6a0588236877a232d22056c3b';


--
-- Name: search_docket_panel pgtrigger_delete_delete_e4921; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_e4921 AFTER DELETE ON public.search_docket_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_e4921();


--
-- Name: TRIGGER pgtrigger_delete_delete_e4921 ON search_docket_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_e4921 ON public.search_docket_panel IS '3b9214ac483e51ed53a6c0586e31525e8d31b7d2';


--
-- Name: search_originatingcourtinformation pgtrigger_delete_delete_ea96d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_ea96d AFTER DELETE ON public.search_originatingcourtinformation FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_ea96d();


--
-- Name: TRIGGER pgtrigger_delete_delete_ea96d ON search_originatingcourtinformation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_ea96d ON public.search_originatingcourtinformation IS 'f6e06aaefc29481bb4ca1c5e327e552ba4455410';


--
-- Name: search_recapdocument_tags pgtrigger_delete_delete_fc2f8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_fc2f8 AFTER DELETE ON public.search_recapdocument_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_fc2f8();


--
-- Name: TRIGGER pgtrigger_delete_delete_fc2f8 ON search_recapdocument_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_fc2f8 ON public.search_recapdocument_tags IS '9576bbbea9a821b0372c469ba523ad5064e33c31';


--
-- Name: disclosures_financialdisclosure pgtrigger_delete_delete_fd8e1; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_delete_delete_fd8e1 AFTER DELETE ON public.disclosures_financialdisclosure FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_delete_delete_fd8e1();


--
-- Name: TRIGGER pgtrigger_delete_delete_fd8e1 ON disclosures_financialdisclosure; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_delete_delete_fd8e1 ON public.disclosures_financialdisclosure IS '3491139b5b8519b484501951aae97d96f4728250';


--
-- Name: search_claim_tags pgtrigger_insert_insert_1c998; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_1c998 AFTER INSERT ON public.search_claim_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_1c998();


--
-- Name: TRIGGER pgtrigger_insert_insert_1c998 ON search_claim_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_1c998 ON public.search_claim_tags IS '825e3576f98d413e758ce70ee393afe96eac31b9';


--
-- Name: people_db_person_race pgtrigger_insert_insert_4de9d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_4de9d AFTER INSERT ON public.people_db_person_race FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_4de9d();


--
-- Name: TRIGGER pgtrigger_insert_insert_4de9d ON people_db_person_race; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_4de9d ON public.people_db_person_race IS '07eec846215436aeb1b4a02f823e7d54d3a9ea31';


--
-- Name: search_docket_panel pgtrigger_insert_insert_500ff; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_500ff AFTER INSERT ON public.search_docket_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_500ff();


--
-- Name: TRIGGER pgtrigger_insert_insert_500ff ON search_docket_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_500ff ON public.search_docket_panel IS 'c0f9ae543d453f72d0dce467dc71d162de5c7af7';


--
-- Name: search_opinioncluster_panel pgtrigger_insert_insert_76236; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_76236 AFTER INSERT ON public.search_opinioncluster_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_76236();


--
-- Name: TRIGGER pgtrigger_insert_insert_76236 ON search_opinioncluster_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_76236 ON public.search_opinioncluster_panel IS 'fa3013dd91633b3a4e2fb434fcda5b989e74668a';


--
-- Name: search_docketentry_tags pgtrigger_insert_insert_7ff94; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_7ff94 AFTER INSERT ON public.search_docketentry_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_7ff94();


--
-- Name: TRIGGER pgtrigger_insert_insert_7ff94 ON search_docketentry_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_7ff94 ON public.search_docketentry_tags IS '185b8a2987d24ec31c16de067f97c62eeaf19a9e';


--
-- Name: search_docket_tags pgtrigger_insert_insert_acaec; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_acaec AFTER INSERT ON public.search_docket_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_acaec();


--
-- Name: TRIGGER pgtrigger_insert_insert_acaec ON search_docket_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_acaec ON public.search_docket_tags IS '7774b046bc2f0071312ab81e910b1e58f12fd865';


--
-- Name: audio_audio_panel pgtrigger_insert_insert_b8778; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_b8778 AFTER INSERT ON public.audio_audio_panel FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_b8778();


--
-- Name: TRIGGER pgtrigger_insert_insert_b8778 ON audio_audio_panel; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_b8778 ON public.audio_audio_panel IS '2f84722ed145863e5187a0488f6aad725927f1ac';


--
-- Name: search_recapdocument_tags pgtrigger_insert_insert_b8d32; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_b8d32 AFTER INSERT ON public.search_recapdocument_tags FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_b8d32();


--
-- Name: TRIGGER pgtrigger_insert_insert_b8d32 ON search_recapdocument_tags; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_b8d32 ON public.search_recapdocument_tags IS '01229805b26ca45fd9b236a2ef66a90f55eefe0c';


--
-- Name: search_opinion_joined_by pgtrigger_insert_insert_c144b; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_c144b AFTER INSERT ON public.search_opinion_joined_by FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_c144b();


--
-- Name: TRIGGER pgtrigger_insert_insert_c144b ON search_opinion_joined_by; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_c144b ON public.search_opinion_joined_by IS '2379eca94d4a3eaa064f56dd27da60a20f6859a9';


--
-- Name: search_court_appeals_to pgtrigger_insert_insert_cec0c; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_cec0c AFTER INSERT ON public.search_court_appeals_to FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_cec0c();


--
-- Name: TRIGGER pgtrigger_insert_insert_cec0c ON search_court_appeals_to; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_cec0c ON public.search_court_appeals_to IS '53f78b19b59d4ee18c41095fd759ba0393aa7631';


--
-- Name: search_opinioncluster_non_participating_judges pgtrigger_insert_insert_d401c; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_insert_insert_d401c AFTER INSERT ON public.search_opinioncluster_non_participating_judges FOR EACH ROW EXECUTE FUNCTION public.pgtrigger_insert_insert_d401c();


--
-- Name: TRIGGER pgtrigger_insert_insert_d401c ON search_opinioncluster_non_participating_judges; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_insert_insert_d401c ON public.search_opinioncluster_non_participating_judges IS '6515eb33fbf31d00fd2e3fdb3051e381a31fdc19';


--
-- Name: search_docket pgtrigger_update_update_068f7; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_068f7 AFTER UPDATE ON public.search_docket FOR EACH ROW WHEN ((((old.appeal_from_id)::text IS DISTINCT FROM (new.appeal_from_id)::text) OR (old.appeal_from_str IS DISTINCT FROM new.appeal_from_str) OR (old.appellate_case_type_information IS DISTINCT FROM new.appellate_case_type_information) OR (old.appellate_fee_status IS DISTINCT FROM new.appellate_fee_status) OR (old.assigned_to_id IS DISTINCT FROM new.assigned_to_id) OR (old.assigned_to_str IS DISTINCT FROM new.assigned_to_str) OR (old.blocked IS DISTINCT FROM new.blocked) OR (old.case_name IS DISTINCT FROM new.case_name) OR (old.case_name_full IS DISTINCT FROM new.case_name_full) OR (old.case_name_short IS DISTINCT FROM new.case_name_short) OR ((old.cause)::text IS DISTINCT FROM (new.cause)::text) OR ((old.court_id)::text IS DISTINCT FROM (new.court_id)::text) OR (old.date_argued IS DISTINCT FROM new.date_argued) OR (old.date_blocked IS DISTINCT FROM new.date_blocked) OR (old.date_cert_denied IS DISTINCT FROM new.date_cert_denied) OR (old.date_cert_granted IS DISTINCT FROM new.date_cert_granted) OR (old.date_filed IS DISTINCT FROM new.date_filed) OR (old.date_last_filing IS DISTINCT FROM new.date_last_filing) OR (old.date_last_index IS DISTINCT FROM new.date_last_index) OR (old.date_reargued IS DISTINCT FROM new.date_reargued) OR (old.date_reargument_denied IS DISTINCT FROM new.date_reargument_denied) OR (old.date_terminated IS DISTINCT FROM new.date_terminated) OR (old.docket_number IS DISTINCT FROM new.docket_number) OR ((old.docket_number_core)::text IS DISTINCT FROM (new.docket_number_core)::text) OR (old.federal_defendant_number IS DISTINCT FROM new.federal_defendant_number) OR ((old.federal_dn_case_type)::text IS DISTINCT FROM (new.federal_dn_case_type)::text) OR ((old.federal_dn_judge_initials_assigned)::text IS DISTINCT FROM (new.federal_dn_judge_initials_assigned)::text) OR ((old.federal_dn_judge_initials_referred)::text IS DISTINCT FROM (new.federal_dn_judge_initials_referred)::text) OR ((old.federal_dn_office_code)::text IS DISTINCT FROM (new.federal_dn_office_code)::text) OR ((old.filepath_ia)::text IS DISTINCT FROM (new.filepath_ia)::text) OR ((old.filepath_ia_json)::text IS DISTINCT FROM (new.filepath_ia_json)::text) OR ((old.filepath_local)::text IS DISTINCT FROM (new.filepath_local)::text) OR (old.ia_date_first_change IS DISTINCT FROM new.ia_date_first_change) OR (old.ia_needs_upload IS DISTINCT FROM new.ia_needs_upload) OR (old.ia_upload_failure_count IS DISTINCT FROM new.ia_upload_failure_count) OR (old.id IS DISTINCT FROM new.id) OR (old.idb_data_id IS DISTINCT FROM new.idb_data_id) OR ((old.jurisdiction_type)::text IS DISTINCT FROM (new.jurisdiction_type)::text) OR ((old.jury_demand)::text IS DISTINCT FROM (new.jury_demand)::text) OR ((old.mdl_status)::text IS DISTINCT FROM (new.mdl_status)::text) OR ((old.nature_of_suit)::text IS DISTINCT FROM (new.nature_of_suit)::text) OR (old.originating_court_information_id IS DISTINCT FROM new.originating_court_information_id) OR ((old.pacer_case_id)::text IS DISTINCT FROM (new.pacer_case_id)::text) OR (old.panel_str IS DISTINCT FROM new.panel_str) OR (old.parent_docket_id IS DISTINCT FROM new.parent_docket_id) OR (old.referred_to_id IS DISTINCT FROM new.referred_to_id) OR (old.referred_to_str IS DISTINCT FROM new.referred_to_str) OR ((old.slug)::text IS DISTINCT FROM (new.slug)::text) OR (old.source IS DISTINCT FROM new.source))) EXECUTE FUNCTION public.pgtrigger_update_update_068f7();


--
-- Name: TRIGGER pgtrigger_update_update_068f7 ON search_docket; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_068f7 ON public.search_docket IS '58cfc45ca31f1c762b2b4142f134519cd4cd3472';


--
-- Name: people_db_abarating pgtrigger_update_update_12775; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_12775 AFTER UPDATE ON public.people_db_abarating FOR EACH ROW WHEN (((old.id IS DISTINCT FROM new.id) OR (old.person_id IS DISTINCT FROM new.person_id) OR ((old.rating)::text IS DISTINCT FROM (new.rating)::text) OR (old.year_rated IS DISTINCT FROM new.year_rated))) EXECUTE FUNCTION public.pgtrigger_update_update_12775();


--
-- Name: TRIGGER pgtrigger_update_update_12775 ON people_db_abarating; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_12775 ON public.people_db_abarating IS '08f526770f5215dded71b6ca405ab987ecb19eb9';


--
-- Name: search_court pgtrigger_update_update_14e3e; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_14e3e AFTER UPDATE ON public.search_court FOR EACH ROW WHEN ((((old.citation_string)::text IS DISTINCT FROM (new.citation_string)::text) OR (old.date_last_pacer_contact IS DISTINCT FROM new.date_last_pacer_contact) OR (old.end_date IS DISTINCT FROM new.end_date) OR ((old.fjc_court_id)::text IS DISTINCT FROM (new.fjc_court_id)::text) OR ((old.full_name)::text IS DISTINCT FROM (new.full_name)::text) OR (old.has_opinion_scraper IS DISTINCT FROM new.has_opinion_scraper) OR (old.has_oral_argument_scraper IS DISTINCT FROM new.has_oral_argument_scraper) OR ((old.id)::text IS DISTINCT FROM (new.id)::text) OR (old.in_use IS DISTINCT FROM new.in_use) OR ((old.jurisdiction)::text IS DISTINCT FROM (new.jurisdiction)::text) OR (old.notes IS DISTINCT FROM new.notes) OR (old.pacer_court_id IS DISTINCT FROM new.pacer_court_id) OR (old.pacer_has_rss_feed IS DISTINCT FROM new.pacer_has_rss_feed) OR (old.pacer_rss_entry_types IS DISTINCT FROM new.pacer_rss_entry_types) OR ((old.parent_court_id)::text IS DISTINCT FROM (new.parent_court_id)::text) OR (old."position" IS DISTINCT FROM new."position") OR ((old.short_name)::text IS DISTINCT FROM (new.short_name)::text) OR (old.start_date IS DISTINCT FROM new.start_date) OR ((old.url)::text IS DISTINCT FROM (new.url)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_14e3e();


--
-- Name: TRIGGER pgtrigger_update_update_14e3e ON search_court; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_14e3e ON public.search_court IS 'a7312b4151d60062b15c3c624ec061088c34c55b';


--
-- Name: search_opinion pgtrigger_update_update_24107; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_24107 AFTER UPDATE ON public.search_opinion FOR EACH ROW WHEN (((old.author_id IS DISTINCT FROM new.author_id) OR (old.author_str IS DISTINCT FROM new.author_str) OR (old.cluster_id IS DISTINCT FROM new.cluster_id) OR ((old.download_url)::text IS DISTINCT FROM (new.download_url)::text) OR (old.extracted_by_ocr IS DISTINCT FROM new.extracted_by_ocr) OR (old.html IS DISTINCT FROM new.html) OR (old.html_anon_2020 IS DISTINCT FROM new.html_anon_2020) OR (old.html_columbia IS DISTINCT FROM new.html_columbia) OR (old.html_lawbox IS DISTINCT FROM new.html_lawbox) OR (old.html_with_citations IS DISTINCT FROM new.html_with_citations) OR (old.id IS DISTINCT FROM new.id) OR (old.joined_by_str IS DISTINCT FROM new.joined_by_str) OR ((old.local_path)::text IS DISTINCT FROM (new.local_path)::text) OR (old.ordering_key IS DISTINCT FROM new.ordering_key) OR (old.page_count IS DISTINCT FROM new.page_count) OR (old.per_curiam IS DISTINCT FROM new.per_curiam) OR (old.plain_text IS DISTINCT FROM new.plain_text) OR ((old.sha1)::text IS DISTINCT FROM (new.sha1)::text) OR ((old.type)::text IS DISTINCT FROM (new.type)::text) OR (old.xml_harvard IS DISTINCT FROM new.xml_harvard))) EXECUTE FUNCTION public.pgtrigger_update_update_24107();


--
-- Name: TRIGGER pgtrigger_update_update_24107 ON search_opinion; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_24107 ON public.search_opinion IS '1cd7e11bdf869c8c34914b5ef4e8f15d9f28866b';


--
-- Name: people_db_retentionevent pgtrigger_update_update_28a9a; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_28a9a AFTER UPDATE ON public.people_db_retentionevent FOR EACH ROW WHEN (((old.date_retention IS DISTINCT FROM new.date_retention) OR (old.id IS DISTINCT FROM new.id) OR (old.position_id IS DISTINCT FROM new.position_id) OR ((old.retention_type)::text IS DISTINCT FROM (new.retention_type)::text) OR (old.unopposed IS DISTINCT FROM new.unopposed) OR (old.votes_no IS DISTINCT FROM new.votes_no) OR (old.votes_no_percent IS DISTINCT FROM new.votes_no_percent) OR (old.votes_yes IS DISTINCT FROM new.votes_yes) OR (old.votes_yes_percent IS DISTINCT FROM new.votes_yes_percent) OR (old.won IS DISTINCT FROM new.won))) EXECUTE FUNCTION public.pgtrigger_update_update_28a9a();


--
-- Name: TRIGGER pgtrigger_update_update_28a9a ON people_db_retentionevent; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_28a9a ON public.people_db_retentionevent IS '2d4b967a62b69659f567e003e1b29c240c10083a';


--
-- Name: people_db_race pgtrigger_update_update_319e8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_319e8 AFTER UPDATE ON public.people_db_race FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION public.pgtrigger_update_update_319e8();


--
-- Name: TRIGGER pgtrigger_update_update_319e8 ON people_db_race; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_319e8 ON public.people_db_race IS '6b32b6b67404a0d9604523642cd144e689f66d12';


--
-- Name: search_bankruptcyinformation pgtrigger_update_update_3d077; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_3d077 AFTER UPDATE ON public.search_bankruptcyinformation FOR EACH ROW WHEN ((((old.chapter)::text IS DISTINCT FROM (new.chapter)::text) OR (old.date_converted IS DISTINCT FROM new.date_converted) OR (old.date_debtor_dismissed IS DISTINCT FROM new.date_debtor_dismissed) OR (old.date_last_to_file_claims IS DISTINCT FROM new.date_last_to_file_claims) OR (old.date_last_to_file_govt IS DISTINCT FROM new.date_last_to_file_govt) OR (old.docket_id IS DISTINCT FROM new.docket_id) OR (old.id IS DISTINCT FROM new.id) OR (old.trustee_str IS DISTINCT FROM new.trustee_str))) EXECUTE FUNCTION public.pgtrigger_update_update_3d077();


--
-- Name: TRIGGER pgtrigger_update_update_3d077 ON search_bankruptcyinformation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_3d077 ON public.search_bankruptcyinformation IS '6d90b79728cffbe523e8319904a1fc7d0bc82e0b';


--
-- Name: people_db_school pgtrigger_update_update_4056a; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_4056a AFTER UPDATE ON public.people_db_school FOR EACH ROW WHEN (((old.ein IS DISTINCT FROM new.ein) OR (old.id IS DISTINCT FROM new.id) OR (old.is_alias_of_id IS DISTINCT FROM new.is_alias_of_id) OR ((old.name)::text IS DISTINCT FROM (new.name)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_4056a();


--
-- Name: TRIGGER pgtrigger_update_update_4056a ON people_db_school; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_4056a ON public.people_db_school IS 'a857ce7bf9b8852fabbb718af336cb62b4b27c96';


--
-- Name: people_db_education pgtrigger_update_update_4231e; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_4231e AFTER UPDATE ON public.people_db_education FOR EACH ROW WHEN ((((old.degree_detail)::text IS DISTINCT FROM (new.degree_detail)::text) OR ((old.degree_level)::text IS DISTINCT FROM (new.degree_level)::text) OR (old.degree_year IS DISTINCT FROM new.degree_year) OR (old.id IS DISTINCT FROM new.id) OR (old.person_id IS DISTINCT FROM new.person_id) OR (old.school_id IS DISTINCT FROM new.school_id))) EXECUTE FUNCTION public.pgtrigger_update_update_4231e();


--
-- Name: TRIGGER pgtrigger_update_update_4231e ON people_db_education; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_4231e ON public.people_db_education IS '7e36b8938bff8f0cb187399b7559da1f7548cc5b';


--
-- Name: disclosures_reimbursement pgtrigger_update_update_4ae7c; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_4ae7c AFTER UPDATE ON public.disclosures_reimbursement FOR EACH ROW WHEN (((old.date_raw IS DISTINCT FROM new.date_raw) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.items_paid_or_provided IS DISTINCT FROM new.items_paid_or_provided) OR (old.location IS DISTINCT FROM new.location) OR (old.purpose IS DISTINCT FROM new.purpose) OR (old.redacted IS DISTINCT FROM new.redacted) OR (old.source IS DISTINCT FROM new.source))) EXECUTE FUNCTION public.pgtrigger_update_update_4ae7c();


--
-- Name: TRIGGER pgtrigger_update_update_4ae7c ON disclosures_reimbursement; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_4ae7c ON public.disclosures_reimbursement IS 'f7768f5f8a23acd4c816a9b3e31e6cbdbb7efd2a';


--
-- Name: search_claimhistory pgtrigger_update_update_51cdc; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_51cdc AFTER UPDATE ON public.search_claimhistory FOR EACH ROW WHEN (((old.attachment_number IS DISTINCT FROM new.attachment_number) OR (old.claim_id IS DISTINCT FROM new.claim_id) OR ((old.claim_doc_id)::text IS DISTINCT FROM (new.claim_doc_id)::text) OR (old.claim_document_type IS DISTINCT FROM new.claim_document_type) OR (old.date_filed IS DISTINCT FROM new.date_filed) OR (old.date_upload IS DISTINCT FROM new.date_upload) OR (old.description IS DISTINCT FROM new.description) OR ((old.document_number)::text IS DISTINCT FROM (new.document_number)::text) OR (old.file_size IS DISTINCT FROM new.file_size) OR ((old.filepath_ia)::text IS DISTINCT FROM (new.filepath_ia)::text) OR ((old.filepath_local)::text IS DISTINCT FROM (new.filepath_local)::text) OR (old.ia_upload_failure_count IS DISTINCT FROM new.ia_upload_failure_count) OR (old.id IS DISTINCT FROM new.id) OR (old.is_available IS DISTINCT FROM new.is_available) OR (old.is_free_on_pacer IS DISTINCT FROM new.is_free_on_pacer) OR (old.is_sealed IS DISTINCT FROM new.is_sealed) OR (old.ocr_status IS DISTINCT FROM new.ocr_status) OR ((old.pacer_case_id)::text IS DISTINCT FROM (new.pacer_case_id)::text) OR (old.pacer_dm_id IS DISTINCT FROM new.pacer_dm_id) OR ((old.pacer_doc_id)::text IS DISTINCT FROM (new.pacer_doc_id)::text) OR (old.page_count IS DISTINCT FROM new.page_count) OR (old.plain_text IS DISTINCT FROM new.plain_text) OR ((old.sha1)::text IS DISTINCT FROM (new.sha1)::text) OR ((old.thumbnail)::text IS DISTINCT FROM (new.thumbnail)::text) OR (old.thumbnail_status IS DISTINCT FROM new.thumbnail_status))) EXECUTE FUNCTION public.pgtrigger_update_update_51cdc();


--
-- Name: TRIGGER pgtrigger_update_update_51cdc ON search_claimhistory; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_51cdc ON public.search_claimhistory IS '8d0db772d098d6edcb3a57b12ae04d596d9f7b86';


--
-- Name: disclosures_gift pgtrigger_update_update_52520; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_52520 AFTER UPDATE ON public.disclosures_gift FOR EACH ROW WHEN (((old.description IS DISTINCT FROM new.description) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.redacted IS DISTINCT FROM new.redacted) OR (old.source IS DISTINCT FROM new.source) OR (old.value IS DISTINCT FROM new.value))) EXECUTE FUNCTION public.pgtrigger_update_update_52520();


--
-- Name: TRIGGER pgtrigger_update_update_52520 ON disclosures_gift; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_52520 ON public.disclosures_gift IS '580bbe79087afa5ac4cb95f2ffbbef5a60f3d076';


--
-- Name: disclosures_spouseincome pgtrigger_update_update_554b8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_554b8 AFTER UPDATE ON public.disclosures_spouseincome FOR EACH ROW WHEN (((old.date_raw IS DISTINCT FROM new.date_raw) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.redacted IS DISTINCT FROM new.redacted) OR (old.source_type IS DISTINCT FROM new.source_type))) EXECUTE FUNCTION public.pgtrigger_update_update_554b8();


--
-- Name: TRIGGER pgtrigger_update_update_554b8 ON disclosures_spouseincome; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_554b8 ON public.disclosures_spouseincome IS '89d864b98dcbcc20915c46257c2d17b9f7145e5d';


--
-- Name: audio_audio pgtrigger_update_update_581d8; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_581d8 AFTER UPDATE ON public.audio_audio FOR EACH ROW WHEN (((old.blocked IS DISTINCT FROM new.blocked) OR (old.case_name IS DISTINCT FROM new.case_name) OR (old.case_name_full IS DISTINCT FROM new.case_name_full) OR (old.case_name_short IS DISTINCT FROM new.case_name_short) OR (old.date_blocked IS DISTINCT FROM new.date_blocked) OR (old.docket_id IS DISTINCT FROM new.docket_id) OR ((old.download_url)::text IS DISTINCT FROM (new.download_url)::text) OR (old.duration IS DISTINCT FROM new.duration) OR ((old.filepath_ia)::text IS DISTINCT FROM (new.filepath_ia)::text) OR (old.ia_upload_failure_count IS DISTINCT FROM new.ia_upload_failure_count) OR (old.id IS DISTINCT FROM new.id) OR (old.judges IS DISTINCT FROM new.judges) OR ((old.local_path_mp3)::text IS DISTINCT FROM (new.local_path_mp3)::text) OR ((old.local_path_original_file)::text IS DISTINCT FROM (new.local_path_original_file)::text) OR (old.processing_complete IS DISTINCT FROM new.processing_complete) OR ((old.sha1)::text IS DISTINCT FROM (new.sha1)::text) OR ((old.source)::text IS DISTINCT FROM (new.source)::text) OR (old.stt_source IS DISTINCT FROM new.stt_source) OR (old.stt_status IS DISTINCT FROM new.stt_status) OR (old.stt_transcript IS DISTINCT FROM new.stt_transcript))) EXECUTE FUNCTION public.pgtrigger_update_update_581d8();


--
-- Name: TRIGGER pgtrigger_update_update_581d8 ON audio_audio; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_581d8 ON public.audio_audio IS '666831a578b79c48bc3a26d307bad268ce4e4e07';


--
-- Name: disclosures_agreement pgtrigger_update_update_6308f; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_6308f AFTER UPDATE ON public.disclosures_agreement FOR EACH ROW WHEN (((old.date_raw IS DISTINCT FROM new.date_raw) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.parties_and_terms IS DISTINCT FROM new.parties_and_terms) OR (old.redacted IS DISTINCT FROM new.redacted))) EXECUTE FUNCTION public.pgtrigger_update_update_6308f();


--
-- Name: TRIGGER pgtrigger_update_update_6308f ON disclosures_agreement; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_6308f ON public.disclosures_agreement IS '6ad97742b1b27327359be0bc4e16a359219bdcd9';


--
-- Name: search_originatingcourtinformation pgtrigger_update_update_65b0a; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_65b0a AFTER UPDATE ON public.search_originatingcourtinformation FOR EACH ROW WHEN (((old.assigned_to_id IS DISTINCT FROM new.assigned_to_id) OR (old.assigned_to_str IS DISTINCT FROM new.assigned_to_str) OR (old.court_reporter IS DISTINCT FROM new.court_reporter) OR (old.date_disposed IS DISTINCT FROM new.date_disposed) OR (old.date_filed IS DISTINCT FROM new.date_filed) OR (old.date_filed_noa IS DISTINCT FROM new.date_filed_noa) OR (old.date_judgment IS DISTINCT FROM new.date_judgment) OR (old.date_judgment_eod IS DISTINCT FROM new.date_judgment_eod) OR (old.date_received_coa IS DISTINCT FROM new.date_received_coa) OR (old.docket_number IS DISTINCT FROM new.docket_number) OR (old.id IS DISTINCT FROM new.id) OR (old.ordering_judge_id IS DISTINCT FROM new.ordering_judge_id) OR (old.ordering_judge_str IS DISTINCT FROM new.ordering_judge_str))) EXECUTE FUNCTION public.pgtrigger_update_update_65b0a();


--
-- Name: TRIGGER pgtrigger_update_update_65b0a ON search_originatingcourtinformation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_65b0a ON public.search_originatingcourtinformation IS '214da086cbf221209cfcc0f11c198f861c0c5d66';


--
-- Name: people_db_politicalaffiliation pgtrigger_update_update_732bd; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_732bd AFTER UPDATE ON public.people_db_politicalaffiliation FOR EACH ROW WHEN (((old.date_end IS DISTINCT FROM new.date_end) OR ((old.date_granularity_end)::text IS DISTINCT FROM (new.date_granularity_end)::text) OR ((old.date_granularity_start)::text IS DISTINCT FROM (new.date_granularity_start)::text) OR (old.date_start IS DISTINCT FROM new.date_start) OR (old.id IS DISTINCT FROM new.id) OR (old.person_id IS DISTINCT FROM new.person_id) OR ((old.political_party)::text IS DISTINCT FROM (new.political_party)::text) OR ((old.source)::text IS DISTINCT FROM (new.source)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_732bd();


--
-- Name: TRIGGER pgtrigger_update_update_732bd ON people_db_politicalaffiliation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_732bd ON public.people_db_politicalaffiliation IS 'bcf04e5f5e19d54eb26453095a64c370b3acc9c9';


--
-- Name: search_docketentry pgtrigger_update_update_75874; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_75874 AFTER UPDATE ON public.search_docketentry FOR EACH ROW WHEN (((old.date_filed IS DISTINCT FROM new.date_filed) OR (old.description IS DISTINCT FROM new.description) OR (old.docket_id IS DISTINCT FROM new.docket_id) OR (old.entry_number IS DISTINCT FROM new.entry_number) OR (old.id IS DISTINCT FROM new.id) OR (old.pacer_sequence_number IS DISTINCT FROM new.pacer_sequence_number) OR ((old.recap_sequence_number)::text IS DISTINCT FROM (new.recap_sequence_number)::text) OR (old.time_filed IS DISTINCT FROM new.time_filed))) EXECUTE FUNCTION public.pgtrigger_update_update_75874();


--
-- Name: TRIGGER pgtrigger_update_update_75874 ON search_docketentry; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_75874 ON public.search_docketentry IS 'ed2f98d6230e1dd82d65afb1f1abe2c4046b825c';


--
-- Name: search_claim pgtrigger_update_update_7f78b; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_7f78b AFTER UPDATE ON public.search_claim FOR EACH ROW WHEN ((((old.amount_claimed)::text IS DISTINCT FROM (new.amount_claimed)::text) OR ((old.claim_number)::text IS DISTINCT FROM (new.claim_number)::text) OR (old.creditor_details IS DISTINCT FROM new.creditor_details) OR ((old.creditor_id)::text IS DISTINCT FROM (new.creditor_id)::text) OR (old.date_claim_modified IS DISTINCT FROM new.date_claim_modified) OR (old.date_last_amendment_entered IS DISTINCT FROM new.date_last_amendment_entered) OR (old.date_last_amendment_filed IS DISTINCT FROM new.date_last_amendment_filed) OR (old.date_original_entered IS DISTINCT FROM new.date_original_entered) OR (old.date_original_filed IS DISTINCT FROM new.date_original_filed) OR (old.description IS DISTINCT FROM new.description) OR (old.docket_id IS DISTINCT FROM new.docket_id) OR ((old.entered_by)::text IS DISTINCT FROM (new.entered_by)::text) OR ((old.filed_by)::text IS DISTINCT FROM (new.filed_by)::text) OR (old.id IS DISTINCT FROM new.id) OR ((old.priority_claimed)::text IS DISTINCT FROM (new.priority_claimed)::text) OR (old.remarks IS DISTINCT FROM new.remarks) OR ((old.secured_claimed)::text IS DISTINCT FROM (new.secured_claimed)::text) OR ((old.status)::text IS DISTINCT FROM (new.status)::text) OR ((old.unsecured_claimed)::text IS DISTINCT FROM (new.unsecured_claimed)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_7f78b();


--
-- Name: TRIGGER pgtrigger_update_update_7f78b ON search_claim; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_7f78b ON public.search_claim IS 'cc31729b28eeb79c74bea1de46fbac27fafb0df2';


--
-- Name: search_citation pgtrigger_update_update_8c292; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_8c292 AFTER UPDATE ON public.search_citation FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION public.pgtrigger_update_update_8c292();


--
-- Name: TRIGGER pgtrigger_update_update_8c292 ON search_citation; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_8c292 ON public.search_citation IS 'ec22d364dbf5727ffdb0a26a5f1200f99601b46a';


--
-- Name: disclosures_noninvestmentincome pgtrigger_update_update_9ce99; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_9ce99 AFTER UPDATE ON public.disclosures_noninvestmentincome FOR EACH ROW WHEN (((old.date_raw IS DISTINCT FROM new.date_raw) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.income_amount IS DISTINCT FROM new.income_amount) OR (old.redacted IS DISTINCT FROM new.redacted) OR (old.source_type IS DISTINCT FROM new.source_type))) EXECUTE FUNCTION public.pgtrigger_update_update_9ce99();


--
-- Name: TRIGGER pgtrigger_update_update_9ce99 ON disclosures_noninvestmentincome; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_9ce99 ON public.disclosures_noninvestmentincome IS '3192e42eec06567f9e9a240799d343352c6fd1b2';


--
-- Name: disclosures_position pgtrigger_update_update_9e43a; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_9e43a AFTER UPDATE ON public.disclosures_position FOR EACH ROW WHEN (((old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.organization_name IS DISTINCT FROM new.organization_name) OR (old."position" IS DISTINCT FROM new."position") OR (old.redacted IS DISTINCT FROM new.redacted))) EXECUTE FUNCTION public.pgtrigger_update_update_9e43a();


--
-- Name: TRIGGER pgtrigger_update_update_9e43a ON disclosures_position; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_9e43a ON public.disclosures_position IS '8071495940cdbcc977a80e4f36f39b65a9ef724b';


--
-- Name: people_db_person pgtrigger_update_update_ae961; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_ae961 AFTER UPDATE ON public.people_db_person FOR EACH ROW WHEN (((old.date_completed IS DISTINCT FROM new.date_completed) OR (old.date_dob IS DISTINCT FROM new.date_dob) OR (old.date_dod IS DISTINCT FROM new.date_dod) OR ((old.date_granularity_dob)::text IS DISTINCT FROM (new.date_granularity_dob)::text) OR ((old.date_granularity_dod)::text IS DISTINCT FROM (new.date_granularity_dod)::text) OR ((old.dob_city)::text IS DISTINCT FROM (new.dob_city)::text) OR ((old.dob_country)::text IS DISTINCT FROM (new.dob_country)::text) OR ((old.dob_state)::text IS DISTINCT FROM (new.dob_state)::text) OR ((old.dod_city)::text IS DISTINCT FROM (new.dod_city)::text) OR ((old.dod_country)::text IS DISTINCT FROM (new.dod_country)::text) OR ((old.dod_state)::text IS DISTINCT FROM (new.dod_state)::text) OR (old.fjc_id IS DISTINCT FROM new.fjc_id) OR ((old.ftm_eid)::text IS DISTINCT FROM (new.ftm_eid)::text) OR (old.ftm_total_received IS DISTINCT FROM new.ftm_total_received) OR ((old.gender)::text IS DISTINCT FROM (new.gender)::text) OR (old.has_photo IS DISTINCT FROM new.has_photo) OR (old.id IS DISTINCT FROM new.id) OR (old.is_alias_of_id IS DISTINCT FROM new.is_alias_of_id) OR ((old.name_first)::text IS DISTINCT FROM (new.name_first)::text) OR ((old.name_last)::text IS DISTINCT FROM (new.name_last)::text) OR ((old.name_middle)::text IS DISTINCT FROM (new.name_middle)::text) OR ((old.name_suffix)::text IS DISTINCT FROM (new.name_suffix)::text) OR ((old.religion)::text IS DISTINCT FROM (new.religion)::text) OR ((old.slug)::text IS DISTINCT FROM (new.slug)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_ae961();


--
-- Name: TRIGGER pgtrigger_update_update_ae961 ON people_db_person; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_ae961 ON public.people_db_person IS 'a149c066c604920a20d3df998c8a282a788082ea';


--
-- Name: search_recapdocument pgtrigger_update_update_af6ad; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_af6ad AFTER UPDATE ON public.search_recapdocument FOR EACH ROW WHEN ((((old.acms_document_guid)::text IS DISTINCT FROM (new.acms_document_guid)::text) OR (old.attachment_number IS DISTINCT FROM new.attachment_number) OR (old.date_upload IS DISTINCT FROM new.date_upload) OR (old.description IS DISTINCT FROM new.description) OR (old.docket_entry_id IS DISTINCT FROM new.docket_entry_id) OR ((old.document_number)::text IS DISTINCT FROM (new.document_number)::text) OR (old.document_type IS DISTINCT FROM new.document_type) OR (old.file_size IS DISTINCT FROM new.file_size) OR ((old.filepath_ia)::text IS DISTINCT FROM (new.filepath_ia)::text) OR ((old.filepath_local)::text IS DISTINCT FROM (new.filepath_local)::text) OR (old.ia_upload_failure_count IS DISTINCT FROM new.ia_upload_failure_count) OR (old.id IS DISTINCT FROM new.id) OR (old.is_available IS DISTINCT FROM new.is_available) OR (old.is_free_on_pacer IS DISTINCT FROM new.is_free_on_pacer) OR (old.is_sealed IS DISTINCT FROM new.is_sealed) OR (old.ocr_status IS DISTINCT FROM new.ocr_status) OR ((old.pacer_doc_id)::text IS DISTINCT FROM (new.pacer_doc_id)::text) OR (old.page_count IS DISTINCT FROM new.page_count) OR (old.plain_text IS DISTINCT FROM new.plain_text) OR ((old.sha1)::text IS DISTINCT FROM (new.sha1)::text) OR ((old.thumbnail)::text IS DISTINCT FROM (new.thumbnail)::text) OR (old.thumbnail_status IS DISTINCT FROM new.thumbnail_status))) EXECUTE FUNCTION public.pgtrigger_update_update_af6ad();


--
-- Name: TRIGGER pgtrigger_update_update_af6ad ON search_recapdocument; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_af6ad ON public.search_recapdocument IS '113a06aaa094dbffa4888d992f7a777c77f05bb0';


--
-- Name: search_courthouse pgtrigger_update_update_b1fd2; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_b1fd2 AFTER UPDATE ON public.search_courthouse FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION public.pgtrigger_update_update_b1fd2();


--
-- Name: TRIGGER pgtrigger_update_update_b1fd2 ON search_courthouse; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_b1fd2 ON public.search_courthouse IS '3ab9fa123c6d47ade6eb9badc4ac5ae37c27be2c';


--
-- Name: search_opinioncluster pgtrigger_update_update_c83f1; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_c83f1 AFTER UPDATE ON public.search_opinioncluster FOR EACH ROW WHEN (((old.arguments IS DISTINCT FROM new.arguments) OR (old.attorneys IS DISTINCT FROM new.attorneys) OR (old.blocked IS DISTINCT FROM new.blocked) OR (old.case_name IS DISTINCT FROM new.case_name) OR (old.case_name_full IS DISTINCT FROM new.case_name_full) OR (old.case_name_short IS DISTINCT FROM new.case_name_short) OR (old.citation_count IS DISTINCT FROM new.citation_count) OR (old.correction IS DISTINCT FROM new.correction) OR (old.cross_reference IS DISTINCT FROM new.cross_reference) OR (old.date_blocked IS DISTINCT FROM new.date_blocked) OR (old.date_filed IS DISTINCT FROM new.date_filed) OR (old.date_filed_is_approximate IS DISTINCT FROM new.date_filed_is_approximate) OR (old.disposition IS DISTINCT FROM new.disposition) OR (old.docket_id IS DISTINCT FROM new.docket_id) OR ((old.filepath_json_harvard)::text IS DISTINCT FROM (new.filepath_json_harvard)::text) OR ((old.filepath_pdf_harvard)::text IS DISTINCT FROM (new.filepath_pdf_harvard)::text) OR (old.headmatter IS DISTINCT FROM new.headmatter) OR (old.headnotes IS DISTINCT FROM new.headnotes) OR (old.history IS DISTINCT FROM new.history) OR (old.id IS DISTINCT FROM new.id) OR (old.judges IS DISTINCT FROM new.judges) OR (old.nature_of_suit IS DISTINCT FROM new.nature_of_suit) OR (old.other_dates IS DISTINCT FROM new.other_dates) OR (old.posture IS DISTINCT FROM new.posture) OR ((old.precedential_status)::text IS DISTINCT FROM (new.precedential_status)::text) OR (old.procedural_history IS DISTINCT FROM new.procedural_history) OR (old.scdb_decision_direction IS DISTINCT FROM new.scdb_decision_direction) OR ((old.scdb_id)::text IS DISTINCT FROM (new.scdb_id)::text) OR (old.scdb_votes_majority IS DISTINCT FROM new.scdb_votes_majority) OR (old.scdb_votes_minority IS DISTINCT FROM new.scdb_votes_minority) OR ((old.slug)::text IS DISTINCT FROM (new.slug)::text) OR ((old.source)::text IS DISTINCT FROM (new.source)::text) OR (old.summary IS DISTINCT FROM new.summary) OR (old.syllabus IS DISTINCT FROM new.syllabus))) EXECUTE FUNCTION public.pgtrigger_update_update_c83f1();


--
-- Name: TRIGGER pgtrigger_update_update_c83f1 ON search_opinioncluster; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_c83f1 ON public.search_opinioncluster IS 'bfad11f5f83c529116ba7ce9b0ed06aac2b5acb5';


--
-- Name: people_db_source pgtrigger_update_update_c9b6d; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_c9b6d AFTER UPDATE ON public.people_db_source FOR EACH ROW WHEN (((old.date_accessed IS DISTINCT FROM new.date_accessed) OR (old.id IS DISTINCT FROM new.id) OR (old.notes IS DISTINCT FROM new.notes) OR (old.person_id IS DISTINCT FROM new.person_id) OR ((old.url)::text IS DISTINCT FROM (new.url)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_c9b6d();


--
-- Name: TRIGGER pgtrigger_update_update_c9b6d ON people_db_source; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_c9b6d ON public.people_db_source IS '07a3f2d9e82866e155768d6aa0d4083871dee5b7';


--
-- Name: disclosures_investment pgtrigger_update_update_d28c7; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_d28c7 AFTER UPDATE ON public.disclosures_investment FOR EACH ROW WHEN (((old.description IS DISTINCT FROM new.description) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR ((old.gross_value_code)::text IS DISTINCT FROM (new.gross_value_code)::text) OR ((old.gross_value_method)::text IS DISTINCT FROM (new.gross_value_method)::text) OR (old.has_inferred_values IS DISTINCT FROM new.has_inferred_values) OR (old.id IS DISTINCT FROM new.id) OR ((old.income_during_reporting_period_code)::text IS DISTINCT FROM (new.income_during_reporting_period_code)::text) OR (old.income_during_reporting_period_type IS DISTINCT FROM new.income_during_reporting_period_type) OR (old.page_number IS DISTINCT FROM new.page_number) OR (old.redacted IS DISTINCT FROM new.redacted) OR (old.transaction_date IS DISTINCT FROM new.transaction_date) OR ((old.transaction_date_raw)::text IS DISTINCT FROM (new.transaction_date_raw)::text) OR (old.transaction_during_reporting_period IS DISTINCT FROM new.transaction_during_reporting_period) OR ((old.transaction_gain_code)::text IS DISTINCT FROM (new.transaction_gain_code)::text) OR (old.transaction_partner IS DISTINCT FROM new.transaction_partner) OR ((old.transaction_value_code)::text IS DISTINCT FROM (new.transaction_value_code)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_d28c7();


--
-- Name: TRIGGER pgtrigger_update_update_d28c7 ON disclosures_investment; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_d28c7 ON public.disclosures_investment IS '5c5230465fe89dfb52e358f9f7a09dc810e587be';


--
-- Name: people_db_position pgtrigger_update_update_de8c0; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_de8c0 AFTER UPDATE ON public.people_db_position FOR EACH ROW WHEN (((old.appointer_id IS DISTINCT FROM new.appointer_id) OR ((old.court_id)::text IS DISTINCT FROM (new.court_id)::text) OR (old.date_confirmation IS DISTINCT FROM new.date_confirmation) OR (old.date_elected IS DISTINCT FROM new.date_elected) OR ((old.date_granularity_start)::text IS DISTINCT FROM (new.date_granularity_start)::text) OR ((old.date_granularity_termination)::text IS DISTINCT FROM (new.date_granularity_termination)::text) OR (old.date_hearing IS DISTINCT FROM new.date_hearing) OR (old.date_judicial_committee_action IS DISTINCT FROM new.date_judicial_committee_action) OR (old.date_nominated IS DISTINCT FROM new.date_nominated) OR (old.date_recess_appointment IS DISTINCT FROM new.date_recess_appointment) OR (old.date_referred_to_judicial_committee IS DISTINCT FROM new.date_referred_to_judicial_committee) OR (old.date_retirement IS DISTINCT FROM new.date_retirement) OR (old.date_start IS DISTINCT FROM new.date_start) OR (old.date_termination IS DISTINCT FROM new.date_termination) OR (old.has_inferred_values IS DISTINCT FROM new.has_inferred_values) OR ((old.how_selected)::text IS DISTINCT FROM (new.how_selected)::text) OR (old.id IS DISTINCT FROM new.id) OR ((old.job_title)::text IS DISTINCT FROM (new.job_title)::text) OR ((old.judicial_committee_action)::text IS DISTINCT FROM (new.judicial_committee_action)::text) OR ((old.location_city)::text IS DISTINCT FROM (new.location_city)::text) OR ((old.location_state)::text IS DISTINCT FROM (new.location_state)::text) OR ((old.nomination_process)::text IS DISTINCT FROM (new.nomination_process)::text) OR ((old.organization_name)::text IS DISTINCT FROM (new.organization_name)::text) OR (old.person_id IS DISTINCT FROM new.person_id) OR ((old.position_type)::text IS DISTINCT FROM (new.position_type)::text) OR (old.predecessor_id IS DISTINCT FROM new.predecessor_id) OR (old.school_id IS DISTINCT FROM new.school_id) OR (old.sector IS DISTINCT FROM new.sector) OR (old.supervisor_id IS DISTINCT FROM new.supervisor_id) OR ((old.termination_reason)::text IS DISTINCT FROM (new.termination_reason)::text) OR (old.voice_vote IS DISTINCT FROM new.voice_vote) OR ((old.vote_type)::text IS DISTINCT FROM (new.vote_type)::text) OR (old.votes_no IS DISTINCT FROM new.votes_no) OR (old.votes_no_percent IS DISTINCT FROM new.votes_no_percent) OR (old.votes_yes IS DISTINCT FROM new.votes_yes) OR (old.votes_yes_percent IS DISTINCT FROM new.votes_yes_percent))) EXECUTE FUNCTION public.pgtrigger_update_update_de8c0();


--
-- Name: TRIGGER pgtrigger_update_update_de8c0 ON people_db_position; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_de8c0 ON public.people_db_position IS 'd877c295481aa7a5d99498db3d07a9a079fd0653';


--
-- Name: disclosures_debt pgtrigger_update_update_e6841; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_e6841 AFTER UPDATE ON public.disclosures_debt FOR EACH ROW WHEN (((old.creditor_name IS DISTINCT FROM new.creditor_name) OR (old.description IS DISTINCT FROM new.description) OR (old.financial_disclosure_id IS DISTINCT FROM new.financial_disclosure_id) OR (old.id IS DISTINCT FROM new.id) OR (old.redacted IS DISTINCT FROM new.redacted) OR ((old.value_code)::text IS DISTINCT FROM (new.value_code)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_e6841();


--
-- Name: TRIGGER pgtrigger_update_update_e6841 ON disclosures_debt; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_e6841 ON public.disclosures_debt IS 'd5f834cefc39cf529ac95dc7cab3f9f34e9d5d6e';


--
-- Name: disclosures_financialdisclosure pgtrigger_update_update_edbb4; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_edbb4 AFTER UPDATE ON public.disclosures_financialdisclosure FOR EACH ROW WHEN (((old.addendum_content_raw IS DISTINCT FROM new.addendum_content_raw) OR (old.addendum_redacted IS DISTINCT FROM new.addendum_redacted) OR (old.download_filepath IS DISTINCT FROM new.download_filepath) OR ((old.filepath)::text IS DISTINCT FROM (new.filepath)::text) OR (old.has_been_extracted IS DISTINCT FROM new.has_been_extracted) OR (old.id IS DISTINCT FROM new.id) OR (old.is_amended IS DISTINCT FROM new.is_amended) OR (old.page_count IS DISTINCT FROM new.page_count) OR (old.person_id IS DISTINCT FROM new.person_id) OR (old.report_type IS DISTINCT FROM new.report_type) OR ((old.sha1)::text IS DISTINCT FROM (new.sha1)::text) OR ((old.thumbnail)::text IS DISTINCT FROM (new.thumbnail)::text) OR (old.thumbnail_status IS DISTINCT FROM new.thumbnail_status) OR (old.year IS DISTINCT FROM new.year))) EXECUTE FUNCTION public.pgtrigger_update_update_edbb4();


--
-- Name: TRIGGER pgtrigger_update_update_edbb4 ON disclosures_financialdisclosure; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_edbb4 ON public.disclosures_financialdisclosure IS '4d28f0244173b550a78fb01244f20dac8d03beda';


--
-- Name: search_tag pgtrigger_update_update_f89d4; Type: TRIGGER; Schema: public; Owner: django
--

CREATE TRIGGER pgtrigger_update_update_f89d4 AFTER UPDATE ON public.search_tag FOR EACH ROW WHEN (((old.id IS DISTINCT FROM new.id) OR ((old.name)::text IS DISTINCT FROM (new.name)::text))) EXECUTE FUNCTION public.pgtrigger_update_update_f89d4();


--
-- Name: TRIGGER pgtrigger_update_update_f89d4 ON search_tag; Type: COMMENT; Schema: public; Owner: django
--

COMMENT ON TRIGGER pgtrigger_update_update_f89d4 ON public.search_tag IS 'd3d07ab1b4071d9170731deb165320dcc220f4ab';


--
-- Name: search_docket a2a62b7d002101ae4c4663cdd1dfc075; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT a2a62b7d002101ae4c4663cdd1dfc075 FOREIGN KEY (originating_court_information_id) REFERENCES public.search_originatingcourtinformation(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: audio_audio audio_audio_docket_id_625f3642919f8934_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio
    ADD CONSTRAINT audio_audio_docket_id_625f3642919f8934_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: audio_audio_panel audio_audio_pa_person_id_afbf0404cefafcc_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio_panel
    ADD CONSTRAINT audio_audio_pa_person_id_afbf0404cefafcc_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: audio_audio_panel audio_audio_panel_audio_id_536de9ffa9ea04fa_fk_audio_audio_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audio_panel
    ADD CONSTRAINT audio_audio_panel_audio_id_536de9ffa9ea04fa_fk_audio_audio_id FOREIGN KEY (audio_id) REFERENCES public.audio_audio(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: audio_audiotranscriptionmetadata audio_audiotranscrip_audio_id_22f57b06_fk_audio_aud; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.audio_audiotranscriptionmetadata
    ADD CONSTRAINT audio_audiotranscrip_audio_id_22f57b06_fk_audio_aud FOREIGN KEY (audio_id) REFERENCES public.audio_audio(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_agreement disclosures_agreemen_financial_disclosure_eb38358a_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_agreement
    ADD CONSTRAINT disclosures_agreemen_financial_disclosure_eb38358a_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_debt disclosures_debt_financial_disclosure_18a78f4c_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_debt
    ADD CONSTRAINT disclosures_debt_financial_disclosure_18a78f4c_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_financialdisclosure disclosures_financia_person_id_83e04c6c_fk_people_db; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_financialdisclosure
    ADD CONSTRAINT disclosures_financia_person_id_83e04c6c_fk_people_db FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_gift disclosures_gift_financial_disclosure_67efabf6_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_gift
    ADD CONSTRAINT disclosures_gift_financial_disclosure_67efabf6_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_investment disclosures_investme_financial_disclosure_ad904849_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_investment
    ADD CONSTRAINT disclosures_investme_financial_disclosure_ad904849_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_noninvestmentincome disclosures_noninves_financial_disclosure_5b351795_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_noninvestmentincome
    ADD CONSTRAINT disclosures_noninves_financial_disclosure_5b351795_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_position disclosures_position_financial_disclosure_b81030c0_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_position
    ADD CONSTRAINT disclosures_position_financial_disclosure_b81030c0_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_reimbursement disclosures_reimburs_financial_disclosure_141ee670_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_reimbursement
    ADD CONSTRAINT disclosures_reimburs_financial_disclosure_141ee670_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: disclosures_spouseincome disclosures_spousein_financial_disclosure_94e0c727_fk_disclosur; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.disclosures_spouseincome
    ADD CONSTRAINT disclosures_spousein_financial_disclosure_94e0c727_fk_disclosur FOREIGN KEY (financial_disclosure_id) REFERENCES public.disclosures_financialdisclosure(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_attorneyorganizationassociation ef7a9c8bc08ab662925cfaa332ca9777; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation
    ADD CONSTRAINT ef7a9c8bc08ab662925cfaa332ca9777 FOREIGN KEY (attorney_organization_id) REFERENCES public.people_db_attorneyorganization(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinioncluster_non_participating_judges opinioncluster_id_3d4a71240680b64c_fk_search_opinioncluster_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_non_participating_judges
    ADD CONSTRAINT opinioncluster_id_3d4a71240680b64c_fk_search_opinioncluster_id FOREIGN KEY (opinioncluster_id) REFERENCES public.search_opinioncluster(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinioncluster_panel opinioncluster_id_7cdb36cb8a6ff7a7_fk_search_opinioncluster_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_panel
    ADD CONSTRAINT opinioncluster_id_7cdb36cb8a6ff7a7_fk_search_opinioncluster_id FOREIGN KEY (opinioncluster_id) REFERENCES public.search_opinioncluster(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_d_appointer_id_7c550f3cea4ba6cd_fk_people_db_position_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_d_appointer_id_7c550f3cea4ba6cd_fk_people_db_position_id FOREIGN KEY (appointer_id) REFERENCES public.people_db_position(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_school people_d_is_alias_of_id_331b4e3cdac3b6f9_fk_people_db_school_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_school
    ADD CONSTRAINT people_d_is_alias_of_id_331b4e3cdac3b6f9_fk_people_db_school_id FOREIGN KEY (is_alias_of_id) REFERENCES public.people_db_school(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_person people_d_is_alias_of_id_53a6eb8a5fb97b64_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person
    ADD CONSTRAINT people_d_is_alias_of_id_53a6eb8a5fb97b64_fk_people_db_person_id FOREIGN KEY (is_alias_of_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_d_predecessor_id_36032dfa6a12d44c_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_d_predecessor_id_36032dfa6a12d44c_fk_people_db_person_id FOREIGN KEY (predecessor_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_attorneyorganizationassociation people_db__attorney_id_1f7b59b220e86ae_fk_people_db_attorney_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation
    ADD CONSTRAINT people_db__attorney_id_1f7b59b220e86ae_fk_people_db_attorney_id FOREIGN KEY (attorney_id) REFERENCES public.people_db_attorney(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_role people_db__attorney_id_1fd94ceb4f0ff93_fk_people_db_attorney_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role
    ADD CONSTRAINT people_db__attorney_id_1fd94ceb4f0ff93_fk_people_db_attorney_id FOREIGN KEY (attorney_id) REFERENCES public.people_db_attorney(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_abarating people_db_aba_person_id_79eb41c300a4a376_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_abarating
    ADD CONSTRAINT people_db_aba_person_id_79eb41c300a4a376_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_attorneyorganizationassociation people_db_attorn_docket_id_50e2b6b752e16618_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_attorneyorganizationassociation
    ADD CONSTRAINT people_db_attorn_docket_id_50e2b6b752e16618_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_education people_db_edu_person_id_688c3a0acdae53a1_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_education
    ADD CONSTRAINT people_db_edu_person_id_688c3a0acdae53a1_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_education people_db_edu_school_id_33cc463e2249bc4a_fk_people_db_school_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_education
    ADD CONSTRAINT people_db_edu_school_id_33cc463e2249bc4a_fk_people_db_school_id FOREIGN KEY (school_id) REFERENCES public.people_db_school(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_partytype people_db_party_party_id_1a614faa135be115_fk_people_db_party_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_partytype
    ADD CONSTRAINT people_db_party_party_id_1a614faa135be115_fk_people_db_party_id FOREIGN KEY (party_id) REFERENCES public.people_db_party(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_partytype people_db_partyt_docket_id_30c09cafa20f361a_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_partytype
    ADD CONSTRAINT people_db_partyt_docket_id_30c09cafa20f361a_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_person_race people_db_per_person_id_2483201f22b91c44_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person_race
    ADD CONSTRAINT people_db_per_person_id_2483201f22b91c44_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_person_race people_db_person__race_id_12bc59b989c779ea_fk_people_db_race_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_person_race
    ADD CONSTRAINT people_db_person__race_id_12bc59b989c779ea_fk_people_db_race_id FOREIGN KEY (race_id) REFERENCES public.people_db_race(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_politicalaffiliation people_db_pol_person_id_7bca4351a3adeccb_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_politicalaffiliation
    ADD CONSTRAINT people_db_pol_person_id_7bca4351a3adeccb_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_db_pos_person_id_796c042ecbe82b71_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_db_pos_person_id_796c042ecbe82b71_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_db_pos_school_id_3cd91cb4ec26941a_fk_people_db_school_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_db_pos_school_id_3cd91cb4ec26941a_fk_people_db_school_id FOREIGN KEY (school_id) REFERENCES public.people_db_school(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_db_position_court_id_7141eee9b516a894_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_db_position_court_id_7141eee9b516a894_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_retentionevent people_db_position_id_7aa450ceb6309890_fk_people_db_position_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_retentionevent
    ADD CONSTRAINT people_db_position_id_7aa450ceb6309890_fk_people_db_position_id FOREIGN KEY (position_id) REFERENCES public.people_db_position(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_role people_db_role_docket_id_43aa88e5be806103_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role
    ADD CONSTRAINT people_db_role_docket_id_43aa88e5be806103_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_role people_db_role_party_id_cc149ce69f8a224_fk_people_db_party_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_role
    ADD CONSTRAINT people_db_role_party_id_cc149ce69f8a224_fk_people_db_party_id FOREIGN KEY (party_id) REFERENCES public.people_db_party(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_source people_db_sou_person_id_547e18a17ea79ec1_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_source
    ADD CONSTRAINT people_db_sou_person_id_547e18a17ea79ec1_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_position people_db_supervisor_id_5e670092b0c8d684_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_position
    ADD CONSTRAINT people_db_supervisor_id_5e670092b0c8d684_fk_people_db_person_id FOREIGN KEY (supervisor_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_criminalcount people_party_type_id_3cd2208b7ab7f97f_fk_people_db_partytype_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcount
    ADD CONSTRAINT people_party_type_id_3cd2208b7ab7f97f_fk_people_db_partytype_id FOREIGN KEY (party_type_id) REFERENCES public.people_db_partytype(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: people_db_criminalcomplaint people_party_type_id_54695b07d9d95d41_fk_people_db_partytype_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.people_db_criminalcomplaint
    ADD CONSTRAINT people_party_type_id_54695b07d9d95d41_fk_people_db_partytype_id FOREIGN KEY (party_type_id) REFERENCES public.people_db_partytype(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_processingqueue r_recap_document_id_24f31a309dadb844_fk_search_recapdocument_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT r_recap_document_id_24f31a309dadb844_fk_search_recapdocument_id FOREIGN KEY (recap_document_id) REFERENCES public.search_recapdocument(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_pacerhtmlfiles reca_content_type_id_111f5d60150ddff1_fk_django_content_type_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerhtmlfiles
    ADD CONSTRAINT reca_content_type_id_111f5d60150ddff1_fk_django_content_type_id FOREIGN KEY (content_type_id) REFERENCES public.django_content_type(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_processingqueue recap_docket_entry_id_2d6224d0bc530cde_fk_search_docketentry_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT recap_docket_entry_id_2d6224d0bc530cde_fk_search_docketentry_id FOREIGN KEY (docket_entry_id) REFERENCES public.search_docketentry(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_emailprocessingqueue_recap_documents recap_emailprocessin_emailprocessingqueue_896acbad_fk_recap_ema; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue_recap_documents
    ADD CONSTRAINT recap_emailprocessin_emailprocessingqueue_896acbad_fk_recap_ema FOREIGN KEY (emailprocessingqueue_id) REFERENCES public.recap_emailprocessingqueue(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_emailprocessingqueue_recap_documents recap_emailprocessin_recapdocument_id_66e16cbf_fk_search_re; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue_recap_documents
    ADD CONSTRAINT recap_emailprocessin_recapdocument_id_66e16cbf_fk_search_re FOREIGN KEY (recapdocument_id) REFERENCES public.search_recapdocument(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_emailprocessingqueue recap_emailprocessingqueue_court_id_83f67bf3_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue
    ADD CONSTRAINT recap_emailprocessingqueue_court_id_83f67bf3_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_emailprocessingqueue recap_emailprocessingqueue_uploader_id_32651a93_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_emailprocessingqueue
    ADD CONSTRAINT recap_emailprocessingqueue_uploader_id_32651a93_fk_auth_user_id FOREIGN KEY (uploader_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_fjcintegrateddatabase recap_fjcintegr_district_id_422635be62a49e48_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_fjcintegrateddatabase
    ADD CONSTRAINT recap_fjcintegr_district_id_422635be62a49e48_fk_search_court_id FOREIGN KEY (district_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_fjcintegrateddatabase recap_fjcintegra_circuit_id_6c29143a8524f734_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_fjcintegrateddatabase
    ADD CONSTRAINT recap_fjcintegra_circuit_id_6c29143a8524f734_fk_search_court_id FOREIGN KEY (circuit_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_pacerfetchqueue recap_pacerfetchqueu_recap_document_id_b9c23829_fk_search_re; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue
    ADD CONSTRAINT recap_pacerfetchqueu_recap_document_id_b9c23829_fk_search_re FOREIGN KEY (recap_document_id) REFERENCES public.search_recapdocument(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_pacerfetchqueue recap_pacerfetchqueue_court_id_1246ddd3_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue
    ADD CONSTRAINT recap_pacerfetchqueue_court_id_1246ddd3_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_pacerfetchqueue recap_pacerfetchqueue_docket_id_371bfcf0_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue
    ADD CONSTRAINT recap_pacerfetchqueue_docket_id_371bfcf0_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_pacerfetchqueue recap_pacerfetchqueue_user_id_a2c0c6f8_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_pacerfetchqueue
    ADD CONSTRAINT recap_pacerfetchqueue_user_id_a2c0c6f8_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_processingqueue recap_processingqu_court_id_58a50ea026638d4b_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT recap_processingqu_court_id_58a50ea026638d4b_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_processingqueue recap_processingqu_docket_id_5efa7fb039db0d_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT recap_processingqu_docket_id_5efa7fb039db0d_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_processingqueue recap_processingqu_uploader_id_62ab26f635761c0f_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_processingqueue
    ADD CONSTRAINT recap_processingqu_uploader_id_62ab26f635761c0f_fk_auth_user_id FOREIGN KEY (uploader_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_rss_rssfeeddata recap_rss_rssfeeddata_court_id_8bd4988e_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeeddata
    ADD CONSTRAINT recap_rss_rssfeeddata_court_id_8bd4988e_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: recap_rss_rssfeedstatus recap_rss_rssfeeds_court_id_41df24dc437f861a_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.recap_rss_rssfeedstatus
    ADD CONSTRAINT recap_rss_rssfeeds_court_id_41df24dc437f861a_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket s_idb_data_id_7696e442c56d310_fk_recap_fjcintegrateddatabase_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT s_idb_data_id_7696e442c56d310_fk_recap_fjcintegrateddatabase_id FOREIGN KEY (idb_data_id) REFERENCES public.recap_fjcintegrateddatabase(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_recapdocument_tags se_recapdocument_id_3a0831353b326f45_fk_search_recapdocument_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument_tags
    ADD CONSTRAINT se_recapdocument_id_3a0831353b326f45_fk_search_recapdocument_id FOREIGN KEY (recapdocument_id) REFERENCES public.search_recapdocument(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_recapdocument searc_docket_entry_id_186f592b9e384e1e_fk_search_docketentry_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument
    ADD CONSTRAINT searc_docket_entry_id_186f592b9e384e1e_fk_search_docketentry_id FOREIGN KEY (docket_entry_id) REFERENCES public.search_docketentry(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_originatingcourtinformation searc_ordering_judge_id_143dbe040e3c8895_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformation
    ADD CONSTRAINT searc_ordering_judge_id_143dbe040e3c8895_fk_people_db_person_id FOREIGN KEY (ordering_judge_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinionscited search__citing_opinion_id_3b336c39ca8491ca_fk_search_opinion_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscited
    ADD CONSTRAINT search__citing_opinion_id_3b336c39ca8491ca_fk_search_opinion_id FOREIGN KEY (citing_opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinion search__cluster_id_48646dd68699f5d6_fk_search_opinioncluster_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion
    ADD CONSTRAINT search__cluster_id_48646dd68699f5d6_fk_search_opinioncluster_id FOREIGN KEY (cluster_id) REFERENCES public.search_opinioncluster(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_bankruptcyinformation search_bankruptcyinf_docket_id_91fa3275_fk_search_do; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_bankruptcyinformation
    ADD CONSTRAINT search_bankruptcyinf_docket_id_91fa3275_fk_search_do FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_citation search_c_cluster_id_c4f8720fbbbd050_fk_search_opinioncluster_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_citation
    ADD CONSTRAINT search_c_cluster_id_c4f8720fbbbd050_fk_search_opinioncluster_id FOREIGN KEY (cluster_id) REFERENCES public.search_opinioncluster(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_claim search_claim_docket_id_b37171a9_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim
    ADD CONSTRAINT search_claim_docket_id_b37171a9_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_claim_tags search_claim_tags_claim_id_2cf554b5_fk_search_claim_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim_tags
    ADD CONSTRAINT search_claim_tags_claim_id_2cf554b5_fk_search_claim_id FOREIGN KEY (claim_id) REFERENCES public.search_claim(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_claim_tags search_claim_tags_tag_id_73b6bd4d_fk_search_tag_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claim_tags
    ADD CONSTRAINT search_claim_tags_tag_id_73b6bd4d_fk_search_tag_id FOREIGN KEY (tag_id) REFERENCES public.search_tag(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_claimhistory search_claimhistory_claim_id_e130e572_fk_search_claim_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_claimhistory
    ADD CONSTRAINT search_claimhistory_claim_id_e130e572_fk_search_claim_id FOREIGN KEY (claim_id) REFERENCES public.search_claim(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_court_appeals_to search_court_appeals_from_court_id_fb09cc1a_fk_search_co; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court_appeals_to
    ADD CONSTRAINT search_court_appeals_from_court_id_fb09cc1a_fk_search_co FOREIGN KEY (from_court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_court_appeals_to search_court_appeals_to_to_court_id_49ac3d9c_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court_appeals_to
    ADD CONSTRAINT search_court_appeals_to_to_court_id_49ac3d9c_fk_search_court_id FOREIGN KEY (to_court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_court search_court_parent_court_id_51ba1d28_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_court
    ADD CONSTRAINT search_court_parent_court_id_51ba1d28_fk_search_court_id FOREIGN KEY (parent_court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_courthouse search_courthouse_court_id_6528f572_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_courthouse
    ADD CONSTRAINT search_courthouse_court_id_6528f572_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket search_d_referred_to_id_7dfd6952e8d18b8c_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_d_referred_to_id_7dfd6952e8d18b8c_fk_people_db_person_id FOREIGN KEY (referred_to_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket search_do_assigned_to_id_185a002e3102ceb_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_do_assigned_to_id_185a002e3102ceb_fk_people_db_person_id FOREIGN KEY (assigned_to_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket search_docke_appeal_from_id_71fecce427985eaf_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docke_appeal_from_id_71fecce427985eaf_fk_search_court_id FOREIGN KEY (appeal_from_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket_panel search_docket__person_id_a216895387ce4ca_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_panel
    ADD CONSTRAINT search_docket__person_id_a216895387ce4ca_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket search_docket_court_id_2d2438b2594e74ba_fk_search_court_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docket_court_id_2d2438b2594e74ba_fk_search_court_id FOREIGN KEY (court_id) REFERENCES public.search_court(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket_panel search_docket_pa_docket_id_6c92125a7941d19b_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_panel
    ADD CONSTRAINT search_docket_pa_docket_id_6c92125a7941d19b_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket search_docket_parent_docket_id_1a514426_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket
    ADD CONSTRAINT search_docket_parent_docket_id_1a514426_fk_search_docket_id FOREIGN KEY (parent_docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket_tags search_docket_ta_docket_id_22afc0b36b1bbca3_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_tags
    ADD CONSTRAINT search_docket_ta_docket_id_22afc0b36b1bbca3_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docket_tags search_docket_tags_tag_id_2f90416e21d2a5cc_fk_search_tag_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docket_tags
    ADD CONSTRAINT search_docket_tags_tag_id_2f90416e21d2a5cc_fk_search_tag_id FOREIGN KEY (tag_id) REFERENCES public.search_tag(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docketentry search_docketentr_docket_id_77c155ebbf826b3_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry
    ADD CONSTRAINT search_docketentr_docket_id_77c155ebbf826b3_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docketentry_tags search_docketentry_id_48bcebf60f001801_fk_search_docketentry_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry_tags
    ADD CONSTRAINT search_docketentry_id_48bcebf60f001801_fk_search_docketentry_id FOREIGN KEY (docketentry_id) REFERENCES public.search_docketentry(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_docketentry_tags search_docketentry_tag_tag_id_6d9cf14285cf89c9_fk_search_tag_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_docketentry_tags
    ADD CONSTRAINT search_docketentry_tag_tag_id_6d9cf14285cf89c9_fk_search_tag_id FOREIGN KEY (tag_id) REFERENCES public.search_tag(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_originatingcourtinformation search_o_assigned_to_id_1cc909cf580febcc_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_originatingcourtinformation
    ADD CONSTRAINT search_o_assigned_to_id_1cc909cf580febcc_fk_people_db_person_id FOREIGN KEY (assigned_to_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinionscited search_op_cited_opinion_id_69ef5d07ce27b76_fk_search_opinion_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscited
    ADD CONSTRAINT search_op_cited_opinion_id_69ef5d07ce27b76_fk_search_opinion_id FOREIGN KEY (cited_opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinioncluster_non_participating_judges search_opinio_person_id_5b0da1008e3e4e3b_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_non_participating_judges
    ADD CONSTRAINT search_opinio_person_id_5b0da1008e3e4e3b_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinion_joined_by search_opinio_person_id_5e482e9ee34284bc_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion_joined_by
    ADD CONSTRAINT search_opinio_person_id_5e482e9ee34284bc_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinioncluster_panel search_opinio_person_id_70c55c02599cc568_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster_panel
    ADD CONSTRAINT search_opinio_person_id_70c55c02599cc568_fk_people_db_person_id FOREIGN KEY (person_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinion_joined_by search_opinion__opinion_id_d92788377db9348_fk_search_opinion_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion_joined_by
    ADD CONSTRAINT search_opinion__opinion_id_d92788377db9348_fk_search_opinion_id FOREIGN KEY (opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinion search_opinion_author_id_a44f4b76b64d99c_fk_people_db_person_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinion
    ADD CONSTRAINT search_opinion_author_id_a44f4b76b64d99c_fk_people_db_person_id FOREIGN KEY (author_id) REFERENCES public.people_db_person(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinioncluster search_opinioncl_docket_id_14b37923614c0da0_fk_search_docket_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinioncluster
    ADD CONSTRAINT search_opinioncl_docket_id_14b37923614c0da0_fk_search_docket_id FOREIGN KEY (docket_id) REFERENCES public.search_docket(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinionscitedbyrecapdocument search_opinionscited_cited_opinion_id_5f0347bb_fk_search_op; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscitedbyrecapdocument
    ADD CONSTRAINT search_opinionscited_cited_opinion_id_5f0347bb_fk_search_op FOREIGN KEY (cited_opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_opinionscitedbyrecapdocument search_opinionscited_citing_document_id_c64b751b_fk_search_re; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_opinionscitedbyrecapdocument
    ADD CONSTRAINT search_opinionscited_citing_document_id_c64b751b_fk_search_re FOREIGN KEY (citing_document_id) REFERENCES public.search_recapdocument(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_parenthetical search_parenthetical_described_opinion_id_ddd408db_fk_search_op; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parenthetical
    ADD CONSTRAINT search_parenthetical_described_opinion_id_ddd408db_fk_search_op FOREIGN KEY (described_opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_parenthetical search_parenthetical_describing_opinion_i_07864494_fk_search_op; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parenthetical
    ADD CONSTRAINT search_parenthetical_describing_opinion_i_07864494_fk_search_op FOREIGN KEY (describing_opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_parenthetical search_parenthetical_group_id_00a7def3_fk_search_pa; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parenthetical
    ADD CONSTRAINT search_parenthetical_group_id_00a7def3_fk_search_pa FOREIGN KEY (group_id) REFERENCES public.search_parentheticalgroup(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_parentheticalgroup search_parenthetical_opinion_id_fd6bb935_fk_search_op; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parentheticalgroup
    ADD CONSTRAINT search_parenthetical_opinion_id_fd6bb935_fk_search_op FOREIGN KEY (opinion_id) REFERENCES public.search_opinion(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_parentheticalgroup search_parenthetical_representative_id_00e5a857_fk_search_pa; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_parentheticalgroup
    ADD CONSTRAINT search_parenthetical_representative_id_00e5a857_fk_search_pa FOREIGN KEY (representative_id) REFERENCES public.search_parenthetical(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_recapdocument_tags search_recapdocument_t_tag_id_1a152aa24561fa85_fk_search_tag_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_recapdocument_tags
    ADD CONSTRAINT search_recapdocument_t_tag_id_1a152aa24561fa85_fk_search_tag_id FOREIGN KEY (tag_id) REFERENCES public.search_tag(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: search_searchquery search_searchquery_user_id_8918791c_fk_auth_user_id; Type: FK CONSTRAINT; Schema: public; Owner: django
--

ALTER TABLE ONLY public.search_searchquery
    ADD CONSTRAINT search_searchquery_user_id_8918791c_fk_auth_user_id FOREIGN KEY (user_id) REFERENCES public.auth_user(id) DEFERRABLE INITIALLY DEFERRED;


--
-- PostgreSQL database dump complete
--

