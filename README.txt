프로젝트 모니터 Python 단일 웹앱 부트스트랩

특징
- npm 필요 없음
- Python만으로 로컬 실행 가능
- templates / static 구조
- Flask 기반
- 추후 Vercel 배포 가능하도록 vercel.json 포함

로컬 실행 방법

1. cmd 열기
2. 아래 순서 실행

cd project-monitor-python
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py

브라우저 확인
- http://127.0.0.1:5000

주요 화면
- /
- /dashboard
- /projects
- /projects/1
