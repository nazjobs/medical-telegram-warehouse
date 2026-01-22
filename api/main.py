from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from .database import get_db

app = FastAPI(title="Kara Solutions Medical Data API")


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Kara Solutions API is Live"}


@app.get("/channels/summary")
def get_channel_summary(db: Session = Depends(get_db)):
    result = db.execute(
        text("""
        SELECT 
            channel_name, 
            COUNT(*) as msg_count, 
            SUM(views) as total_views 
        FROM fct_messages 
        GROUP BY channel_name
    """)
    ).fetchall()

    return [
        {"channel": r.channel_name, "count": r.msg_count, "views": r.total_views}
        for r in result
    ]


@app.get("/images/detections")
def get_detection_stats(db: Session = Depends(get_db)):
    result = db.execute(
        text("""
        SELECT 
            image_class, 
            COUNT(*) as count 
        FROM fct_messages 
        WHERE image_class IS NOT NULL 
        GROUP BY image_class
    """)
    ).fetchall()

    return [{"class": r.image_class, "count": r.count} for r in result]
