from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import Optional
from pathlib import Path
from fastapi.responses import JSONResponse

router = APIRouter()

UPLOAD_DIR = Path("bank_statements")
PASSWORD_DIR = Path("passwords")
UPLOAD_DIR.mkdir(exist_ok=True)
PASSWORD_DIR.mkdir(exist_ok=True)

@router.post("/upload-statement")
async def upload_pdf(
    username: str = Form(...),
    file: UploadFile = File(...),
    password: Optional[str] = Form(None)
):
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

    file_path = UPLOAD_DIR / f"{username}.pdf"
    password_path = PASSWORD_DIR / f"{username}.txt"

    if file_path.exists():
        raise HTTPException(status_code=400, detail=f"User '{username}' already uploaded a statement.")

    try:
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        if password:
            with open(password_path, "w") as f:
                f.write(password)

        return JSONResponse(
            status_code=200,
            content={
                "message": "✅ File uploaded successfully.",
                "password_provided": password is not None
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Error saving file: {str(e)}")
