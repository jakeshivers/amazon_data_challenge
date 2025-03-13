from contextlib import contextmanager
from sqlalchemy import create_engine, Column, Integer, Numeric, String, ForeignKey, CheckConstraint, TIMESTAMP
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship

# Database setup
DATABASE_URL = "postgresql://brett:mypassword@localhost:5432/amazon_products_db"
engine = create_engine(DATABASE_URL)

# Ensure that the session factory is correctly bound to the engine
SessionLocal = sessionmaker(bind=engine)
Session = scoped_session(SessionLocal)

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        print(f"Session connected as: {engine.url}")  # Debugging info
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")  # Log errors
        raise
    finally:
        session.close()
        Session.remove()  # Ensure cleanup for scoped_session

Base = declarative_base()

class ProductScalars(Base):
    __tablename__ = "product_scalars"
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(255), ForeignKey("product.product_id", ondelete="CASCADE"), nullable=False)
    category = Column(String(255), nullable=False)
    rating = Column(Numeric(3, 2), nullable=False)
    rating_count = Column(Integer, nullable=False)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint("rating BETWEEN 0 AND 5", name="check_rating"),
        CheckConstraint("rating_count >= 0", name="check_rating_count"),
    )

class Products(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(255), unique=True, nullable=False)
    product_name = Column(String(500), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    scalars = relationship("ProductScalars", backref="products", cascade="all, delete")  # Full cascade

# Create tables if they don't exist
Base.metadata.create_all(engine)
print("Tables created successfully!")
