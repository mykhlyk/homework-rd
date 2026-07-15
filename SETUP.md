# Налаштування середовища для курсу Data Engineering

Цей гайд готує ваш комп'ютер до роботи на курсі: редактор, Python, Git,
container runtime. Зробіть його **один раз перед першим заняттям** — далі
кожне заняття лише доустановлює залежності всередині проєкту.

Жоден інструмент тут **не вимагає платної ліцензії**. Ми свідомо
не використовуємо Docker Desktop (платний для великих компаній) і взагалі
не ставимо desktop-клієнтів — керувати контейнерами будемо з розширень VS Code.

---

## Що ми встановимо

| Інструмент | Навіщо | З якого заняття потрібен |
|---|---|---|
| **uv** | менеджер Python-версій і пакетів | 01 |
| **Python 3.12** | мова, якою працює весь курс | 02 |
| **Git** | version control, здавання ДЗ | 01 |
| **GitHub акаунт + SSH** | здавання ДЗ через Pull Request | 01 |
| **VS Code** + розширення | редактор для всіх прикладів | 01 |
| **Container runtime** (Colima / Docker Engine) | контейнери | 07 *(поставте заздалегідь)* |

**Налаштуєте пізніше, окремими інструкціями — зараз не потрібно:**
AWS CLI та акаунт (заняття 09, 18), Java/JDK для Apache Spark (заняття 11+).

---

## Як користуватися гайдом

1. Знайдіть свою платформу — **Трек A (macOS)**, **Трек B (Windows + WSL)**
   або **Трек C (Linux)** — і пройдіть лише його.
2. Потім виконайте **Спільну секцію** (Git, GitHub, VS Code, перевірка) —
   вона однакова для всіх.

> **Фіксована версія Python — 3.12.** Її розуміють усі бібліотеки курсу,
> включно з PySpark і dbt. Не ставте новішу: PySpark може ще не підтримувати її.

---

# Трек A — macOS

### A1. Homebrew

Homebrew — менеджер пакетів для macOS. Офіційний сайт та інструкція:
**https://brew.sh**

Встановлення (команда з офіційного сайту):

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Після інсталяції термінал підкаже два рядки `Next steps` — виконайте їх,
щоб додати `brew` у `PATH` (на Apple Silicon це
`eval "$(/opt/homebrew/bin/brew shellenv)"`).

### A2. uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Перезапустіть термінал. Офіційна документація: https://docs.astral.sh/uv/

### A3. Python 3.12 через uv

```bash
uv python install 3.12
```

### A4. Git

```bash
brew install git
```

### A5. Container runtime — Colima (без desktop-клієнта)

Colima запускає Docker-сумісний рушій у легкій віртуалці, керується з терміналу,
ліцензія MIT — жодних обмежень.

```bash
brew install colima docker docker-compose
colima start --cpu 4 --memory 8
```

`colima start` автоматично налаштовує `docker` так, що команда `docker`
та розширення VS Code одразу його бачать. Зупинити: `colima stop`.

→ Перейдіть до **Спільної секції**.

---

# Трек B — Windows (через WSL2)

На Windows ми працюємо **не в самому Windows, а всередині Linux через WSL2**.
Це дає те саме середовище, що й у викладача, і прибирає більшість проблем
сумісності. Усі команди після кроку B0 виконуються в терміналі Ubuntu.

### B0. Встановити WSL2 *(додатковий крок лише для Windows)*

Передумови: Windows 10 версії 2004+ або Windows 11, увімкнена віртуалізація
в BIOS/UEFI (зазвичай вже увімкнена).

Відкрийте **PowerShell від імені адміністратора** і виконайте:

```powershell
wsl --install
```

Команда поставить WSL2 і дистрибутив Ubuntu. **Перезавантажте комп'ютер.**
Після перезавантаження Ubuntu попросить придумати Linux-логін і пароль —
запам'ятайте пароль, він знадобиться для `sudo`.

Оновіть систему всередині Ubuntu:

```bash
sudo apt update && sudo apt upgrade -y
```

> Далі **всі команди виконуються в терміналі Ubuntu**, а не в PowerShell.

### B1. uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Перезапустіть термінал. Документація: https://docs.astral.sh/uv/

### B2. Python 3.12 через uv

```bash
uv python install 3.12
```

### B3. Git

```bash
sudo apt install -y git
```

### B4. Container runtime — Docker Engine всередині WSL (без Docker Desktop)

Ставимо саме **Docker Engine** (безкоштовний, Apache-2.0), а не Docker Desktop.
Офіційний скрипт встановлення:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

Щоб демон стартував сам при кожному запуску WSL, увімкніть **systemd**.
Перевірте, чи він уже працює:

```bash
ps -p 1 -o comm=          # якщо виводить "systemd" — пропустіть наступний крок
```

Якщо там не `systemd`, відредагуйте `/etc/wsl.conf`:

```bash
sudo nano /etc/wsl.conf
```

і додайте:

```ini
[boot]
systemd=true
```

Збережіть, потім у **PowerShell** виконайте `wsl --shutdown` і знову відкрийте Ubuntu.

Закрийте й відкрийте термінал ще раз (щоб застосувалася група `docker`)
та ввімкніть автозапуск демона — тепер це робиться **один раз**:

```bash
sudo systemctl enable --now docker
```

Розширення Docker у VS Code (через WSL) бачитиме демон автоматично.

→ Перейдіть до **Спільної секції**.

---

# Трек C — Linux (Ubuntu / Debian)

### C1. uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Перезапустіть термінал. Документація: https://docs.astral.sh/uv/

### C2. Python 3.12 через uv

```bash
uv python install 3.12
```

### C3. Git

```bash
sudo apt update && sudo apt install -y git
```

### C4. Container runtime — Docker Engine (без Docker Desktop)

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

Перелогіньтеся (щоб застосувалася група `docker`) і ввімкніть автозапуск демона:

```bash
sudo systemctl enable --now docker
```

→ Перейдіть до **Спільної секції**.

---

# Спільна секція — для всіх платформ

Виконайте після свого треку.

### 1. Налаштувати ім'я в Git

```bash
git config --global user.name "Ім'я Прізвище"
git config --global user.email "you@example.com"
git config --global init.defaultBranch main
```

### 2. GitHub: акаунт і SSH-ключ

ДЗ ми здаємо через Pull Request, тож потрібен акаунт на
**https://github.com** (якщо ще немає — зареєструйтеся).

Згенеруйте SSH-ключ і додайте його до акаунта:

```bash
ssh-keygen -t ed25519 -C "you@example.com"     # тричі Enter
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub                        # скопіюйте увесь рядок
```

Скопійований ключ вставте на GitHub: **Settings → SSH and GPG keys →
New SSH key**. Перевірте з'єднання:

```bash
ssh -T git@github.com
```

Має з'явитися привітання `Hi <username>!`.

### 3. VS Code і розширення

Завантажте VS Code: **https://code.visualstudio.com**

Встановіть розширення (вкладка Extensions, `Ctrl/Cmd + Shift + X`):

- **Python** (`ms-python.python`)
- **Jupyter** (`ms-toolsai.jupyter`)
- **Container Tools** / **Docker** (`ms-azuretools.vscode-docker`) — керування контейнерами без desktop-клієнта
- **Even Better TOML** (`tamasfe.even-better-toml`)
- **WSL** (`ms-vscode-remote.remote-wsl`) — **лише Windows**

> **Windows:** встановлюйте VS Code на боці Windows (не в Ubuntu).
> Щоб відкрити проєкт усередині WSL, у терміналі Ubuntu перейдіть у папку
> проєкту й виконайте `code .` — VS Code під'єднається до WSL автоматично.

### 4. Зафіксувати версію Python у проєкті

Перейдіть у папку курсу та закріпіть Python 3.12 — створиться файл
`.python-version`, який буде єдиним джерелом правди про версію:

```bash
uv python pin 3.12
```

### 5. Перевірити, що все працює

```bash
uv --version
uv run python --version      # → Python 3.12.x
git --version
docker --version
docker run hello-world       # має вивести "Hello from Docker!"
code --version
```

Якщо всі команди відпрацювали — середовище готове.

### 6. Робочий процес здавання ДЗ (коротко)

ДЗ здаються **не Pull Request'ом у репозиторій курсу**, а через ваш
**власний репозиторій на GitHub** — так вам не потрібні права на запис
у репозиторій викладача:

1. Склонуйте репозиторій курсу (тільки для читання):
   ```bash
   git clone git@github.com:robot-dreams-code/UA_DATA-ENGINEERING_KHOROSHYKH-2-hw.git
   ```
   (репозиторій на GitHub: https://github.com/robot-dreams-code/UA_DATA-ENGINEERING_KHOROSHYKH-2-hw/tree/main)
2. Створіть **свій власний порожній** репозиторій на GitHub:
   - відкрийте **https://github.com/new**;
   - введіть назву репозиторію (наприклад, `de-course-homework`);
   - оберіть **Public** або **Private** — на ваш розсуд;
   - **НЕ** вмикайте жодну з опцій «Add a README file», «.gitignore», «Choose a
     license» — репозиторій має лишитися повністю порожнім, інакше `git push`
     у наступному кроці впаде через конфлікт історій;
   - натисніть **Create repository**.

   Альтернатива через GitHub CLI (якщо встановлено `gh` і виконано `gh auth login`):
   ```bash
   gh repo create your-repo-name --private --source=. --remote=origin --push
   ```
   Ця команда сама створює репозиторій на GitHub, додає `origin` і пушить —
   після неї крок 3 не потрібен.
3. Перемкніть remote вашого клону на цей новий репозиторій і запуште туди
   поточний стан курсу:
   ```bash
   git remote remove origin
   git remote add origin git@github.com:<your-username>/<your-repo>.git
   git push -u origin main
   ```
4. Додайте репозиторій курсу як **`upstream`** — так ви зможете підтягувати
   нові заняття (нові файли й папки), які викладач додає новими комітами:
   ```bash
   git remote add upstream git@github.com:robot-dreams-code/UA_DATA-ENGINEERING_KHOROSHYKH-2-hw.git
   ```
   Перед початком роботи над кожним новим заняттям підтягніть свіжий контент —
   **обов'язково з гілки `main`**, а не з гілки домашки:
   ```bash
   git switch main
   git fetch upstream
   git merge upstream/main      # або: git pull upstream main
   git push origin main
   ```
   Нові файли від викладача merge додає без конфліктів. Конфлікт можливий
   лише якщо ви змінили той самий файл, що й викладач — тоді Git позначить
   спірні місця маркерами `<<<<<<<`/`>>>>>>>`: залиште потрібну версію,
   потім `git add <файл>` і `git commit`. Якщо щось пішло не так —
   `git merge --abort` скасує merge і поверне все як було.
5. Додайте викладача (**`@desireoftheother`**) як **maintainer** свого
   репозиторію, щоб він міг рев'ювити ваші PR:
   - у своєму репозиторії на GitHub відкрийте **Settings → Collaborators and
     teams**;
   - натисніть **Add people**;
   - введіть `desireoftheother` і оберіть акаунт зі списку;
   - оберіть роль **Maintain** (може рев'ювити й мерджити PR, без прав
     видалити репозиторій);
   - натисніть **Add desireoftheother to this repository**.

   Це надсилає запрошення — доступ з'явиться лише після того, як викладач
   його **прийме** (лист або GitHub → Notifications).
6. На кожне заняття — окрема гілка: `git checkout -b homework-01`.
7. Зробіть зміни, далі `git add .`, `git commit -m "..."`,
   `git push -u origin homework-01`.
8. У **своєму** репозиторії відкрийте **Pull Request** (гілка → `main`)
   і додайте `@desireoftheother` як **reviewer**.

Деталі workflow для конкретного заняття будуть у його `README`.

---

## Якщо щось пішло не так

- **`uv: command not found`** — перезапустіть термінал; uv додає себе в `PATH`
  лише для нових сесій.
- **`docker: command not found` або `permission denied` на Docker (Linux/WSL)** —
  ви не перелогінилися після `usermod -aG docker`. Закрийте термінал і відкрийте знову.
- **`Cannot connect to the Docker daemon` (WSL)** — спершу `sudo systemctl start docker`.
  Якщо `systemctl` каже, що systemd неактивний — увімкніть його в `/etc/wsl.conf`
  (`[boot]\nsystemd=true`), виконайте `wsl --shutdown` у PowerShell і відкрийте Ubuntu знову.
- **Colima не стартує (macOS)** — перевірте, що достатньо ресурсів,
  і спробуйте `colima delete && colima start`.
- **VS Code не бачить Python 3.12** — відкрийте Command Palette
  (`Ctrl/Cmd + Shift + P`) → `Python: Select Interpreter` → виберіть `.venv` проєкту.
- **Windows: `wsl --install` не спрацював** — увімкніть віртуалізацію в BIOS/UEFI
  та компонент «Virtual Machine Platform» у Windows Features, тоді повторіть.
- **Найголовніше:** якщо не можете знайти рішення -- пишіть у чат в Slack!
