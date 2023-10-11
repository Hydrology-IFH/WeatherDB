import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from datetime import datetime
from typing import Optional
from typing_extensions import Annotated
from geoalchemy2 import Geometry

# define custom types
sint = Annotated[int, 2]
str30 = Annotated[str, 30]

# define base class for all database tables
class Base(DeclarativeBase):
    registry = sa.orm.registry(
        type_annotation_map={
            sint: sa.SmallInteger(),
            datetime: sa.TIMESTAMP(),
            float: sa.FLOAT(),
            str30: sa.CHAR(30),
        }
    )

# declare all database tables
#----------------------------
class MetaN(Base):
    __tablename__ = 'meta_n'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    is_real: Mapped[bool] = mapped_column(default=True)
    raw_from: Mapped[Optional[datetime]]
    raw_until: Mapped[Optional[datetime]]
    hist_until: Mapped[Optional[datetime]]
    qc_from: Mapped[Optional[datetime]]
    qc_until: Mapped[Optional[datetime]]
    filled_from: Mapped[Optional[datetime]]
    filled_until: Mapped[Optional[datetime]]
    corr_from: Mapped[Optional[datetime]]
    corr_until: Mapped[Optional[datetime]]
    last_imp_from: Mapped[Optional[datetime]]
    last_imp_until: Mapped[Optional[datetime]]
    last_imp_qc: Mapped[bool] = mapped_column(default=False)
    last_imp_filled: Mapped[bool] = mapped_column(default=False)
    last_imp_corr: Mapped[bool] = mapped_column(default=False)
    stationshoehe: Mapped[int]
    stationsname: Mapped[str30]
    bundesland: Mapped[str30]
    horizon: Mapped[float]
    richter_class: Mapped[str] = mapped_column(sa.String())
    quot_filled_hyras: Mapped[float]
    quot_filled_regnie: Mapped[float]
    quot_filled_dwd_grid: Mapped[float]
    quot_corr_filled: Mapped[float]
    geometry: Mapped[str] = mapped_column(Geometry('POINT', 4326))
    geometry_utm: Mapped[str] = mapped_column(Geometry('POINT', 25832))


class MetaND(Base):
    __tablename__ = 'meta_n_d'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    is_real: Mapped[bool] = mapped_column(default=True)
    raw_from: Mapped[Optional[datetime]]
    raw_until: Mapped[Optional[datetime]]
    hist_until: Mapped[Optional[datetime]]
    qc_from: Mapped[Optional[datetime]]
    qc_until: Mapped[Optional[datetime]]
    filled_from: Mapped[Optional[datetime]]
    filled_until: Mapped[Optional[datetime]]
    last_imp_from: Mapped[Optional[datetime]]
    last_imp_until: Mapped[Optional[datetime]]
    last_imp_filled: Mapped[bool] = mapped_column(default=False)
    stationshoehe: Mapped[int]
    stationsname: Mapped[str30]
    bundesland: Mapped[str30]
    geometry: Mapped[str] = mapped_column(Geometry('POINT', 4326))
    geometry_utm: Mapped[str] = mapped_column(Geometry('POINT', 25832))


class MetaET(Base):
    __tablename__ = 'meta_et'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    is_real: Mapped[bool] = mapped_column(default=True)
    raw_from: Mapped[Optional[datetime]]
    raw_until: Mapped[Optional[datetime]]
    hist_until: Mapped[Optional[datetime]]
    qc_until: Mapped[Optional[datetime]]
    qc_from: Mapped[Optional[datetime]]
    filled_from: Mapped[Optional[datetime]]
    filled_until: Mapped[Optional[datetime]]
    last_imp_from: Mapped[Optional[datetime]]
    last_imp_until: Mapped[Optional[datetime]]
    last_imp_qc: Mapped[bool] = mapped_column(default=False)
    last_imp_filled: Mapped[bool] = mapped_column(default=False)
    stationshoehe: Mapped[int]
    stationsname: Mapped[str30]
    bundesland: Mapped[str30]
    geometry: Mapped[str] = mapped_column(Geometry('POINT', 4326))
    geometry_utm: Mapped[str] = mapped_column(Geometry('POINT', 25832))


class MetaT(Base):
    __tablename__ = 'meta_t'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    is_real: Mapped[bool] = mapped_column(default=True)
    raw_from: Mapped[Optional[datetime]]
    raw_until: Mapped[Optional[datetime]]
    hist_until: Mapped[Optional[datetime]]
    qc_until: Mapped[Optional[datetime]]
    qc_from: Mapped[Optional[datetime]]
    filled_from: Mapped[Optional[datetime]]
    filled_until: Mapped[Optional[datetime]]
    last_imp_from: Mapped[Optional[datetime]]
    last_imp_until: Mapped[Optional[datetime]]
    last_imp_qc: Mapped[bool] = mapped_column(default=False)
    last_imp_filled: Mapped[bool] = mapped_column(default=False)
    stationshoehe: Mapped[int]
    stationsname: Mapped[str30]
    bundesland: Mapped[str30]
    geometry: Mapped[str] = mapped_column(Geometry('POINT', 4326))
    geometry_utm: Mapped[str] = mapped_column(Geometry('POINT', 25832))


class RawFiles(Base):
    __tablename__ = 'raw_files'

    para: Mapped[str] = mapped_column(primary_key=True)
    filepath: Mapped[str] = mapped_column(primary_key=True)
    modtime: Mapped[datetime]


class DropedStations(Base):
    __tablename__ = 'droped_stations'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    para: Mapped[str] = mapped_column(sa.CHAR(3), primary_key=True)
    why: Mapped[str] = mapped_column(sa.Text())
    timestamp: Mapped[datetime] = mapped_column(server_default=func.now())


class ParaVariables(Base):
    __tablename__ = 'para_variables'

    para: Mapped[str] = mapped_column(sa.String(3), primary_key=True)
    start_tstp_last_imp: Mapped[Optional[datetime]]
    max_tstp_last_imp: Mapped[Optional[datetime]]


class RichterValues(Base):
    __tablename__ = 'richter_values'

    precipitation_typ: Mapped[str] = mapped_column(sa.Text(), primary_key=True)
    temperaturbereich: Mapped[str] = mapped_column(sa.Text())
    e: Mapped[float]
    b_no_protection: Mapped[float] = mapped_column(name="b_no-protection")
    b_little_protection: Mapped[float] = mapped_column(name="b_little-protection")
    b_protected: Mapped[float]
    b_heavy_protection: Mapped[float] = mapped_column(name="b_heavy-protection")


class StationsRasterValues(Base):
    __tablename__ = 'stations_raster_values'

    station_id: Mapped[int] = mapped_column(primary_key=True)
    n_dwd_wihj: Mapped[sint]
    n_dwd_sohj: Mapped[sint]
    n_dwd_year: Mapped[sint]
    t_dwd_year: Mapped[int]
    et_dwd_year: Mapped[sint]
    dist_dwd: Mapped[sint]
    n_regnie_wihj: Mapped[sint]
    n_regnie_sohj: Mapped[sint]
    n_regnie_year: Mapped[sint]
    dist_regnie: Mapped[sint]
    n_hyras_wihj: Mapped[sint]
    n_hyras_sohj: Mapped[sint]
    n_hyras_year: Mapped[sint]
    dist_hyras: Mapped[sint]
    s_r_f: Mapped[float]
    r__s: Mapped[float]
    dist_sol: Mapped[sint]
    geometry: Mapped[str] = mapped_column(Geometry('POINT', 25832))


class NeededDownloadTime(Base):
    __tablename__ = 'needed_download_time'

    timestamp: Mapped[] = mapped_column(sa.TIMESTAMP(), server_default=func.now(), primary_key=True)
    quantity: Mapped[] = mapped_column(sa.Integer(), nullable=False)
    aggregate: Mapped[] = mapped_column(sa.String(), nullable=False)
    timespan: Mapped[] = mapped_column(sa.Interval(), nullable=False)
    zip: Mapped[] = mapped_column(sa.Boolean(), nullable=False)
    pc: Mapped[] = mapped_column(sa.String(), nullable=False)
    duration: Mapped[] = mapped_column(sa.Interval(), nullable=False)
    output_size: Mapped[] = mapped_column(sa.Integer(), nullable=False)

