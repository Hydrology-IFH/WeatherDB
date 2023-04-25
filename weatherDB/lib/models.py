import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()


class DropedStations(Base):

    __tablename__ = 'droped_stations'

    station_id = sa.Column(int4(), primary_key=True)
    para = sa.Column(bpchar(3), primary_key=True)
    why = sa.Column(sa.Text())
    timestamp = sa.Column(sa.TIMESTAMP())


class MetaEt(Base):

    __tablename__ = 'meta_et'

    station_id = sa.Column(int4(), primary_key=True)
    is_real = sa.Column(sa.Boolean(), nullable=False, server_default='true')
    raw_from = sa.Column(sa.TIMESTAMP())
    raw_until = sa.Column(sa.TIMESTAMP())
    filled_from = sa.Column(sa.TIMESTAMP())
    filled_until = sa.Column(sa.TIMESTAMP())
    last_imp_from = sa.Column(sa.TIMESTAMP())
    last_imp_until = sa.Column(sa.TIMESTAMP())
    last_imp_qc = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    last_imp_filled = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    stationshoehe = sa.Column(int4())
    stationsname = sa.Column(sa.String())
    bundesland = sa.Column(sa.String())
    hist_until = sa.Column(sa.TIMESTAMP())
    qc_from = sa.Column(sa.TIMESTAMP())
    qc_until = sa.Column(sa.TIMESTAMP())


class MetaN(Base):

    __tablename__ = 'meta_n'

    station_id = sa.Column(int4(), primary_key=True)
    is_real = sa.Column(sa.Boolean(), nullable=False, server_default='true')
    raw_from = sa.Column(sa.TIMESTAMP())
    raw_until = sa.Column(sa.TIMESTAMP())
    filled_from = sa.Column(sa.TIMESTAMP())
    filled_until = sa.Column(sa.TIMESTAMP())
    last_imp_from = sa.Column(sa.TIMESTAMP())
    last_imp_until = sa.Column(sa.TIMESTAMP())
    last_imp_qc = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    last_imp_filled = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    last_imp_corr = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    stationshoehe = sa.Column(int4())
    stationsname = sa.Column(sa.String())
    bundesland = sa.Column(sa.String())
    richter_class = sa.Column(sa.String())
    horizon = sa.Column(float4())
    quot_filled_regnie = sa.Column(float4())
    quot_filled_dwd_grid = sa.Column(float4())
    quot_corr_filled = sa.Column(float4())
    corr_from = sa.Column(sa.TIMESTAMP())
    corr_until = sa.Column(sa.TIMESTAMP())
    hist_until = sa.Column(sa.TIMESTAMP())
    quot_filled_hyras = sa.Column(float8())
    qc_from = sa.Column(sa.TIMESTAMP())
    qc_until = sa.Column(sa.TIMESTAMP())


class MetaND(Base):

    __tablename__ = 'meta_n_d'

    station_id = sa.Column(int4(), primary_key=True)
    is_real = sa.Column(sa.Boolean(), nullable=False, server_default='true')
    raw_from = sa.Column(sa.TIMESTAMP())
    raw_until = sa.Column(sa.TIMESTAMP())
    filled_from = sa.Column(sa.TIMESTAMP())
    filled_until = sa.Column(sa.TIMESTAMP())
    last_imp_from = sa.Column(sa.TIMESTAMP())
    last_imp_until = sa.Column(sa.TIMESTAMP())
    last_imp_filled = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    stationshoehe = sa.Column(int4())
    stationsname = sa.Column(sa.String())
    bundesland = sa.Column(sa.String())
    hist_until = sa.Column(sa.TIMESTAMP())
    qc_from = sa.Column(sa.TIMESTAMP())
    qc_until = sa.Column(sa.TIMESTAMP())


class MetaT(Base):

    __tablename__ = 'meta_t'

    station_id = sa.Column(int4(), primary_key=True)
    is_real = sa.Column(sa.Boolean(), nullable=False, server_default='true')
    raw_from = sa.Column(sa.TIMESTAMP())
    raw_until = sa.Column(sa.TIMESTAMP())
    filled_from = sa.Column(sa.TIMESTAMP())
    filled_until = sa.Column(sa.TIMESTAMP())
    last_imp_from = sa.Column(sa.TIMESTAMP())
    last_imp_until = sa.Column(sa.TIMESTAMP())
    last_imp_qc = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    last_imp_filled = sa.Column(sa.Boolean(), nullable=False, server_default='false')
    stationshoehe = sa.Column(int4())
    stationsname = sa.Column(sa.String())
    bundesland = sa.Column(sa.String())
    hist_until = sa.Column(sa.TIMESTAMP())
    qc_from = sa.Column(sa.TIMESTAMP())
    qc_until = sa.Column(sa.TIMESTAMP())


class NeededDownloadTime(Base):

    __tablename__ = 'needed_download_time'

    timestamp = sa.Column(sa.TIMESTAMP(), server_default=func.now(), primary_key=True)
    quantity = sa.Column(int4(), nullable=False)
    aggregate = sa.Column(sa.String(), nullable=False)
    timespan = sa.Column(interval(), nullable=False)
    zip = sa.Column(sa.Boolean(), nullable=False)
    pc = sa.Column(sa.String(), nullable=False)
    duration = sa.Column(interval(), nullable=False)
    output_size = sa.Column(int4(), nullable=False)


class ParaVariables(Base):

    __tablename__ = 'para_variables'

    para = sa.Column(bpchar(3), primary_key=True)
    start_tstp_last_imp = sa.Column(sa.TIMESTAMP())
    max_tstp_last_imp = sa.Column(sa.TIMESTAMP())


class RawFiles(Base):

    __tablename__ = 'raw_files'

    para = sa.Column(sa.String(), primary_key=True)
    filepath = sa.Column(sa.String(), primary_key=True)
    modtime = sa.Column(sa.TIMESTAMP(), nullable=False)


class RichterValues(Base):

    __tablename__ = 'richter_values'

    precipitation_typ = sa.Column(sa.Text(), primary_key=True)
    temperaturbereich = sa.Column(sa.Text())
    e = sa.Column(float8())
    b_no-protection = sa.Column(float8())
    b_little-protection = sa.Column(float8())
    b_protected = sa.Column(float8())
    b_heavy-protection = sa.Column(float8())


class StationsRasterValues(Base):

    __tablename__ = 'stations_raster_values'

    station_id = sa.Column(int4(), primary_key=True)
    n_dwd_wihj = sa.Column(int2())
    n_dwd_sohj = sa.Column(int2())
    n_dwd_year = sa.Column(int2())
    t_dwd_year = sa.Column(int4())
    et_dwd_year = sa.Column(int2())
    dist_dwd = sa.Column(int2())
    n_regnie_wihj = sa.Column(int2())
    n_regnie_sohj = sa.Column(int2())
    n_regnie_year = sa.Column(int2())
    dist_regnie = sa.Column(int2())
    n_hyras_wihj = sa.Column(int2())
    n_hyras_sohj = sa.Column(int2())
    n_hyras_year = sa.Column(int2())
    dist_hyras = sa.Column(int2())
    s_r_f = sa.Column(float8())
    r__s = sa.Column(float8())
    dist_sol = sa.Column(int2())
