
import asyncio
import getpass
import sys

from sqlalchemy import select

from app.core.config import Settings
from app.core.security import hash_password
from app.models import User

# Ajuste este import se o projeto utilizar outro módulo de sessão.
from app.core.database import async_session_maker


async def create_user() -> None:
    email = input("E-mail do usuário: ").strip().lower()

    if not email or "@" not in email:
        print("Erro: informe um e-mail válido.")
        sys.exit(1)

    password = getpass.getpass("Senha: ")
    confirm_password = getpass.getpass("Confirme a senha: ")

    if len(password) < 8:
        print("Erro: a senha deve ter pelo menos 8 caracteres.")
        sys.exit(1)

    if password != confirm_password:
        print("Erro: as senhas não coincidem.")
        sys.exit(1)

    async with async_session_maker() as session:
        existing_user = await session.scalar(
            select(User).where(User.email == email)
        )

        if existing_user:
            print(f"Erro: o usuário {email} já existe.")
            return

        user = User(
            email=email,
            hashed_password=hash_password(password),
        )

        session.add(user)

        try:
            await session.commit()
            await session.refresh(user)
            print("\nUsuário criado com sucesso!")
            print(f"ID: {user.id}")
            print(f"E-mail: {user.email}")
            print(f"Ativo: {user.is_active}")
        except Exception:
            await session.rollback()
            raise


if __name__ == "__main__":
    asyncio.run(create_user())
