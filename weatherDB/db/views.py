import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement

from .models import StationMATimeserie, StationMARaster, ModelBase

__all__ = [
    "StationMAQuotientView",
    "StationKindQuotientView"
]

# View Bases
class CreateView(Executable, ClauseElement):
    inherit_cache = True
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

@compiles(CreateView)
def compile_create_view(element, compiler, **kwargs):
    return "CREATE OR REPLACE VIEW %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True)
    )

class DropView(Executable, ClauseElement):
    inherit_cache = True
    def __init__(self, name, cascade=False, if_exists=True):
        self.name = name
        self.cascade = cascade
        self.if_exists = if_exists

@compiles(DropView)
def compile_drop_view(element, compiler, **kwargs):
    return "DROP VIEW %s%s%s" % (
        "IF EXISTS " if element.if_exists else "",
        element.name,
        " CASCADE" if element.cascade else ""
    )

class ViewBase(ModelBase):
    __view_selectable__ = None
    __abstract__ = True

    @classmethod
    def create_view(cls, target, connection, **kwargs):
        if cls.__view_selectable__ is None:
            raise NotImplementedError("No selectable defined for view. Please define a class variable \"__view_selectable__\"")
        view = CreateView(cls.__tablename__, cls.__view_selectable__)
        connection.execute(view)
        connection.commit()

    @classmethod
    def drop_view(cls, target, connection, **kwargs):
        drop = DropView(cls.__tablename__, if_exists=True, cascade=True)
        connection.execute(drop)
        connection.commit()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.metadata._remove_table(
            cls.__tablename__,
            cls.__table_args__.get("schema", "public"))
        sa.event.listen(cls.metadata, 'after_create', cls.create_view)
        sa.event.listen(cls.metadata, 'before_drop', cls.drop_view)

        # add a views section to metadata
        if hasattr(cls, "__view_selectable__"):
            if not hasattr(cls.metadata, "views"):
                cls.metadata.views = [cls]
            elif cls not in cls.metadata.views:
                cls.metadata.views.append(cls)

# declare all database views
# --------------------------
class StationMATimeserieRasterQuotientView(ViewBase):
    __tablename__ = 'station_ma_timeseries_raster_quotient_view'
    __table_args__ = dict(
        schema='public',
        comment="The multi annual mean values of the stations timeseries divided by the multi annual raster values for the maximum available timespan.",
        extend_existing = True)

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The DWD-ID of the station.")
    parameter: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The parameter of the station. e.g. 'p', 'et'")
    kind: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The kind of the timeserie. e.g. 'raw', 'filled', 'corr'")
    raster_key: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The name of the raster. e.g. 'dwd' or 'hyras'")
    value: Mapped[float] = mapped_column(
        comment="The multi annual value of the yearly mean value of the station to the multi annual mean value of the raster.")

    __view_selectable__ = sa\
        .select(
            StationMATimeserie.station_id,
            StationMATimeserie.parameter,
            StationMATimeserie.kind,
            StationMARaster.raster_key,
            StationMARaster.term,
            sa.case(
                (StationMARaster.value is not None,
                 StationMATimeserie.value / StationMARaster.value),
                else_=None).label("value")
        ).select_from(
            StationMATimeserie.__table__.outerjoin(
                StationMARaster.__table__,
                sa.and_(
                    StationMATimeserie.station_id == StationMARaster.station_id,
                    StationMATimeserie.parameter == StationMARaster.parameter
                )
            )
        ).where(
            StationMATimeserie.parameter.in_(["p", "et"])
        )

class StationKindQuotientView(ViewBase):
    __tablename__ = 'station_kind_quotient_view'
    __table_args__ = dict(
        schema='public',
        comment="The quotient between different kinds of multi annual mean timeseries values.",
        extend_existing = True)

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The DWD-ID of the station.")
    parameter: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The parameter of the station. e.g. 'p', 'p_d', 'et'")
    kind_numerator: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The kind of the timeserie for the numerator. e.g. 'raw', 'filled', 'corr'")
    kind_denominator: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The kind of the timeserie for the denominator. e.g. 'raw', 'filled', 'corr'")
    value: Mapped[float] = mapped_column(
        comment="The quotient between the numerator and the nominator kind of the complete timeserie.")

    __view_selectable__ = sa\
            .select(
                (smt1:=sa.orm.aliased(StationMATimeserie, name="smt1")).station_id,
                smt1.parameter,
                smt1.kind.label("kind_numerator"),
                (smt2:=sa.orm.aliased(StationMATimeserie, name="smt2")).kind.label("kind_denominator"),
                (smt1.value/smt2.value).label("value")
            ).select_from(
                sa.join(
                    smt1,
                    smt2,
                    sa.and_(
                        smt1.station_id == smt2.station_id,
                        smt1.parameter == smt2.parameter
                    )
                )
            ).where(
                sa.and_(smt1.parameter.in_(["p", "p_d", "et"]),
                        smt1.kind != smt2.kind,
            )
        )