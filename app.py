import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, render_template, abort, request, redirect, url_for, flash, jsonify, session
from upstash_redis import Redis


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "260407")

redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)


def is_master():
    return session.get("is_master", False)


def require_master():
    if not is_master():
        flash("마스터 권한이 필요합니다.")
        return False
    return True


@app.context_processor
def inject_master_flag():
    return {
        "is_master": session.get("is_master", False)
    }


STAGE_MASTER = [
    {"stage_order": "1", "stage_name": "작업지시서"},
    {"stage_order": "2", "stage_name": "PM지정"},
    {"stage_order": "3", "stage_name": "설명회"},
    {"stage_order": "4", "stage_name": "팀구성"},
    {"stage_order": "5", "stage_name": "착수보고서작성"},
    {"stage_order": "6", "stage_name": "승인협의"},
    {"stage_order": "7", "stage_name": "점검회의"},
    {"stage_order": "7-1", "stage_name": "CHECK SHEET"},
    {"stage_order": "8", "stage_name": "업체선정"},
    {"stage_order": "9", "stage_name": "완료보고"},
]

ACTUAL_STAGE_COUNT = len(STAGE_MASTER)

REQUIRED_FIELDS_BY_STAGE = {
    "1": [],
    "2": [],
    "3": ["planned_date"],
    "4": ["planned_date"],
    "5": ["planned_date"],
    "6": ["planned_date"],
    "7": ["planned_date"],
    "7-1": ["planned_date"],
    "8": ["planned_date"],
    "9": ["planned_date"],
}

STAGE_NOTE_HELP = {
    "1": "작업지시서 발행 후 품질팀 입력",
    "2": "생관 PM선정 공유 후 품질팀 입력",
    "3": "회의록 결재 후 실적일 입력",
    "4": "오른쪽 상단 팀구성원 입력",
    "5": "보고서 결재 후 실적일 입력",
    "6": "회의록 PDM 저장 후 실적일 입력",
    "7": "회의록 결재 후 실적일 입력",
    "7-1": "작성 체크시트 기입 예) 설계업무 / 외주설계",
    "8": "선정업체 기입 예) 조립:CMT, 두원 / 설치:CMT / 제어:나라",
    "9": "보고서 결재 후 실적일 입력",
}


def redis_get_json(key, default):
    value = redis.get(key)
    if value is None:
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value


def redis_set_json(key, value):
    redis.set(key, json.dumps(value, ensure_ascii=False))


def load_projects():
    return redis_get_json("pm:projects", [])


def save_projects(projects):
    redis_set_json("pm:projects", projects)


def load_project_stages(project_id):
    return redis_get_json(f"pm:stages:{project_id}", [])


def save_project_stages(project_id, stages):
    redis_set_json(f"pm:stages:{project_id}", stages)


def load_project_teams(project_id):
    return redis_get_json(
        f"pm:teams:{project_id}",
        {
            "team_rows": [
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            ]
        },
    )


def save_project_teams(project_id, teams):
    redis_set_json(f"pm:teams:{project_id}", teams)


def load_project_history(project_id):
    return redis_get_json(f"pm:history:{project_id}", [])


def save_project_history(project_id, history_rows):
    redis_set_json(f"pm:history:{project_id}", history_rows)


def find_project(project_id: int, include_deleted: bool = False):
    projects = load_projects()
    for project in projects:
        if project["id"] == project_id:
            if not include_deleted and project.get("is_deleted", False):
                return None
            return project
    return None


def get_next_project_id():
    projects = load_projects()
    if not projects:
        return 1
    return max(project["id"] for project in projects) + 1


def generate_project_code():
    projects = load_projects()
    year_prefix = datetime.today().strftime("%y")
    same_year_codes = []

    for project in projects:
        code = str(project.get("code", "")).strip()
        if code.startswith(year_prefix) and code[2:].isdigit():
            same_year_codes.append(int(code[2:]))

    next_number = (max(same_year_codes) + 1) if same_year_codes else 1
    return f"{year_prefix}{next_number:03d}"


def get_fixed_assignee(stage_order: str):
    if stage_order == "1":
        return "영업팀"
    if stage_order == "2":
        return "생관팀"
    if stage_order == "3":
        return "영업팀"
    if stage_order == "4":
        return "생관팀"
    if stage_order == "5":
        return "PM"
    if stage_order == "6":
        return "PM"
    if stage_order == "7":
        return "PM"
    if stage_order == "7-1":
        return "설계팀"
    if stage_order == "8":
        return "구매팀"
    if stage_order == "9":
        return "PM"
    return ""


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

def now_kst():
    return datetime.now(ZoneInfo("Asia/Seoul"))

def add_days(date_str, days):
    d = parse_date(date_str)
    if not d:
        return None
    return (d + timedelta(days=days)).strftime("%Y-%m-%d")

def get_stage_deadline(stage_order, planned_date):
    """
    단계별 지연 판단 기준일 계산
    """

    if not planned_date:
        return None

    # 설명회(3단계)는 계획일 +3일 까지 허용
    if str(stage_order) == "3":
        return add_days(planned_date, 3)

    return planned_date

def get_project_history(project_id: int):
    return load_project_history(project_id)


def add_stage_change_history(
    project_id: int,
    stage_order: str,
    field_name: str,
    field_label: str,
    old_value,
    new_value,
    changed_by: str,
    change_reason: str,
):
    history_rows = get_project_history(project_id)
    history_rows.append(
        {
            "stage_order": stage_order,
            "field_name": field_name,
            "field_label": field_label,
            "old_value": old_value or "",
            "new_value": new_value or "",
            "changed_by": changed_by.strip(),
            "change_reason": change_reason.strip(),
            "changed_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    save_project_history(project_id, history_rows)


def get_stage_history_rows(project_id: int, stage_order: str):
    history_rows = get_project_history(project_id)
    rows = [
        row for row in history_rows
        if str(row.get("stage_order")) == str(stage_order)
        and row.get("field_name") in ["planned_date", "actual_date", "approval"]
    ]
    rows.sort(key=lambda x: x["changed_at"], reverse=True)
    return rows


def find_stage_in_project(project_id: int, stage_order: str):
    stages = load_project_stages(project_id)
    return next((stage for stage in stages if stage["stage_order"] == stage_order), None)


def get_project_team(project_id: int):
    return load_project_teams(project_id)


def normalize_team_rows(pm_list, design_list, machine_list, control_list, sales_list):
    rows = []

    for pm, design, machine, control, sales in zip(
        pm_list, design_list, machine_list, control_list, sales_list
    ):
        row = {
            "pm": pm.strip(),
            "design": design.strip(),
            "machine": machine.strip(),
            "control": control.strip(),
            "sales": sales.strip(),
        }
        if any(row.values()):
            rows.append(row)

    if not rows:
        rows = [
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
        ]

    return rows


def has_missing_required_fields(stage_order, assignee_name, planned_date, actual_date, approval_date, note):
    required_fields = REQUIRED_FIELDS_BY_STAGE.get(stage_order, [])

    values = {
        "assignee_name": assignee_name.strip() if assignee_name else "",
        "planned_date": planned_date,
        "actual_date": actual_date,
        "approval_date": approval_date,
        "note": note.strip() if note else "",
    }

    for field_name in required_fields:
        if not values.get(field_name):
            return True

    return False


def compute_stage_status(stage_order, assignee_name, planned_date, actual_date, approval_date, note, saved_status, is_not_applicable=False):
    if is_not_applicable:
        return "해당없음"

    planned = parse_date(planned_date)
    deadline_date = get_stage_deadline(stage_order, planned_date)
    deadline = parse_date(deadline_date)
    actual = parse_date(actual_date)
    approval = parse_date(approval_date)
    today = datetime.today().date()

    if actual and approval:
        return "완료"

    if actual and not approval:
        return "승인대기"

    if has_missing_required_fields(
        stage_order,
        assignee_name,
        planned_date,
        actual_date,
        approval_date,
        note,
    ):
        return "누락"

    if deadline and not actual:
        if deadline < today:
            return "지연"
        return "진행"

    return "누락"


def merge_stages(project_id: int):
    saved_list = load_project_stages(project_id)
    saved_map = {stage["stage_order"]: stage for stage in saved_list}
    merged = []

    for master in STAGE_MASTER:
        saved = saved_map.get(master["stage_order"], {})

        planned_date = saved.get("planned_date")
        actual_date = saved.get("actual_date")
        approval_date = saved.get("approval_date")
        note = saved.get("note", "")
        assignee_name = get_fixed_assignee(master["stage_order"])
        saved_status = saved.get("status", "")
        is_not_applicable = saved.get("is_not_applicable", False)

        status = compute_stage_status(
            master["stage_order"],
            assignee_name,
            planned_date,
            actual_date,
            approval_date,
            note,
            saved_status,
            is_not_applicable,
        )

        # 6~9단계는 5단계가 완료/해당없음 전까지는 미착수
        # 5단계가 준비되면 이후는 기본 계산값(누락/승인대기/완료)을 그대로 사용
        if master["stage_order"] in ["6", "7", "7-1", "8", "9"] and not is_not_applicable:
            stage5 = next((s for s in merged if s["stage_order"] == "5"), None)
            stage5_ready = stage5 and (
            stage5.get("status") in ["완료", "해당없음", "승인대기"]
            or bool(stage5.get("actual_date"))
        )

            if not stage5_ready:
                status = "미착수"

        merged.append(
            {
                "stage_order": master["stage_order"],
                "stage_name": master["stage_name"],
                "assignee_name": assignee_name,
                "planned_date": planned_date,
                "actual_date": actual_date,
                "approval_date": approval_date,
                "note": note,
                "note_help": STAGE_NOTE_HELP.get(master["stage_order"], ""),
                "status": status,
                "is_not_applicable": is_not_applicable,
            }
        )

    return merged


def recompute_project(project_id):
    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id), None)
    if not project or project.get("is_deleted", False):
        return

    merged = merge_stages(project_id)

    project["is_delayed"] = any(s["status"] == "지연" for s in merged)
    project["is_missing"] = any(s["status"] == "누락" for s in merged)

    current_candidates = [
        s for s in merged
        if s["status"] in ["승인대기", "지연", "누락", "진행"]
    ]

    if current_candidates:
        current_stage = current_candidates[0]
    else:
        completed_candidates = [s for s in merged if s["status"] == "완료"]
        if completed_candidates:
            current_stage = completed_candidates[-1]
        else:
            current_stage = merged[0]

    project["current_stage_order"] = current_stage["stage_order"]
    project["current_stage"] = current_stage["stage_name"]

        # ⭐ 보류 최우선
    if project.get("is_hold", False):
        project["status"] = "보류"

    elif any(s["status"] == "지연" for s in merged):
        project["status"] = "지연"

    elif any(s["status"] == "승인대기" for s in merged):
        project["status"] = "승인대기"

    elif all(s["status"] in ["완료", "해당없음"] for s in merged):
        project["status"] = "완료"

    elif any(s["status"] == "진행" for s in merged):
        project["status"] = "진행"

    else:
        project["status"] = "누락"

    save_projects(projects)


def recompute_all_projects():
    projects = load_projects()
    for project in projects:
        if project.get("is_deleted", False):
            continue
        recompute_project(project["id"])


def get_progress_color(progress_percent: int, is_delayed: bool):
    if is_delayed:
        return "red"
    if progress_percent >= 70:
        return "green"
    if progress_percent >= 30:
        return "blue"
    return "gray"


def build_stage_mini_view(stages):
    items = []
    for stage in stages:
        status = stage["status"]

        if status == "완료":
            color = "green"
        elif status == "진행":
            color = "blue"
        elif status == "승인대기":
            color = "purple"
        elif status == "지연":
            color = "red"
        elif status == "누락":
            color = "yellow"
        elif status == "미착수":
            color = "gray"
        elif status == "해당없음":
            color = "lightgray"
        else:
            color = "gray"

        items.append(
            {
                "label": stage["stage_order"],
                "title": f'{stage["stage_order"]} {stage["stage_name"]} / {stage.get("assignee_name", "-") or "-"} / {status}',
                "color": color,
            }
        )
    return items


def enrich_project(project):
    stages = merge_stages(project["id"])
    
    today = datetime.today().date()

    for stage in stages:
        deadline_date = get_stage_deadline(
            stage["stage_order"],
            stage.get("planned_date"),
        )
        deadline = parse_date(deadline_date)

        stage["delay_deadline"] = deadline_date
        stage["delay_days"] = 0

        if stage["status"] == "지연" and deadline:
            stage["delay_days"] = max((today - deadline).days, 0)

    completed_count = sum(1 for stage in stages if stage["status"] in ["완료", "해당없음"])
    progress_text = f"{completed_count}/{ACTUAL_STAGE_COUNT}"
    progress_percent = int((completed_count / ACTUAL_STAGE_COUNT) * 100)
    current_stage_display = f'{project["current_stage_order"]} {project["current_stage"]}'

    enriched = dict(project)
    enriched.setdefault("is_hold", False)
    enriched.setdefault("hold_requested", False)
    enriched.setdefault("hold_request_by", "")
    enriched.setdefault("hold_request_reason", "")
    enriched.setdefault("hold_request_memo", "")
    enriched.setdefault("hold_request_at", None)
    enriched.setdefault("hold_reason", "")
    enriched.setdefault("hold_start_date", None)
    enriched.setdefault("hold_end_date", None)
    enriched.setdefault("hold_history", [])
    enriched.setdefault("release_requested", False)
    enriched.setdefault("release_request_by", "")
    enriched.setdefault("release_request_reason", "")
    enriched.setdefault("release_request_at", None)
    enriched["completed_count"] = completed_count
    enriched["progress_text"] = progress_text
    enriched["progress_percent"] = progress_percent
    enriched["progress_color"] = "gray" if project.get("is_hold") else get_progress_color(progress_percent, project.get("is_delayed", False))
    enriched["current_stage_display"] = current_stage_display
    enriched["is_missing"] = project.get("is_missing", False)
    enriched["stage_mini_view"] = build_stage_mini_view(stages)
    enriched["stages"] = stages
    return enriched

def safe_code_sort(project):
    try:
        return int(str(project.get("code", "")).strip())
    except (TypeError, ValueError):
        return 999999


def get_filtered_projects():
    keyword = request.args.get("keyword", "").strip().lower()
    status = request.args.get("status", "").strip()
    delay = request.args.get("delay", "").strip()

    enriched_projects = [
        enrich_project(project)
        for project in load_projects()
        if not project.get("is_deleted", False)
    ]
    filtered = []

    for project in enriched_projects:
        searchable = " ".join(
            [
                str(project.get("code", "")),
                str(project.get("name", "")),
                str(project.get("customer", "")),
                str(project.get("location", "")),
                str(project.get("pm_name", "")),
                str(project.get("current_stage", "")),
                str(project.get("current_stage_order", "")),
            ]
        ).lower()

        if keyword and keyword not in searchable:
            continue

        if status == "누락" and not project.get("is_missing", False):
            continue
        elif status == "지연" and not project.get("is_delayed", False):
            continue
        elif status == "보류":
            if project.get("status") != "보류":
                continue
        elif status and status not in ["누락", "지연", "보류"] and project["status"] != status:
            continue

        if delay == "Y":
            if not project["is_delayed"] or project.get("status") == "보류":
                continue

        if delay == "N":
            if project["is_delayed"]:
                continue

        filtered.append(project)

    return filtered, keyword, status, delay


@app.route("/")
def home():
    return redirect(url_for("dashboard"))


@app.route("/master/login", methods=["POST"])
def master_login():
    password = request.form.get("master_password", "").strip()

    if password == MASTER_PASSWORD:
        session["is_master"] = True
        flash("마스터 모드가 활성화되었습니다.")
    else:
        flash("마스터 비밀번호가 올바르지 않습니다.")

    return redirect(request.referrer or url_for("dashboard"))


@app.route("/master/logout", methods=["POST"])
def master_logout():
    session.pop("is_master", None)
    flash("마스터 모드가 해제되었습니다.")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    projects = [
        enrich_project(project)
        for project in load_projects()
        if not project.get("is_deleted", False)
    ]

    projects = sorted(projects, key=safe_code_sort)

    stage_approval_projects = [
        p for p in projects
        if any(stage["status"] == "승인대기" for stage in p["stages"])
    ]

    hold_requested_projects = [
        p for p in projects
        if p.get("hold_requested")
    ]

    approval_pending_projects = []
    release_requested_projects = [
        p for p in projects
        if p.get("release_requested")
    ]

    for p in stage_approval_projects:
        item = dict(p)
        item["approval_text"] = f'{p["current_stage_display"]} / 단계 승인 필요'
        approval_pending_projects.append(item)

    for p in hold_requested_projects:
        item = dict(p)
        item["approval_text"] = "보류 신청 / 승인 필요"
        approval_pending_projects.append(item)

    for p in release_requested_projects:
        item = dict(p)
        item["approval_text"] = "보류 해제 요청 / 승인 필요"
        approval_pending_projects.append(item)

    summary = {
        "total": len(projects),
        "in_progress": sum(1 for p in projects if p.get("status") == "진행"),
        "approval_pending": len(approval_pending_projects),
        "completed": sum(1 for p in projects if p.get("status") == "완료"),
        "hold": sum(1 for p in projects if p.get("status") == "보류"),
        "missing": sum(1 for p in projects if p.get("is_missing") and p.get("status") != "보류"),
        "delayed": sum(1 for p in projects if p.get("is_delayed") and p.get("status") != "보류"),
    }

    delayed_projects = [
        p for p in projects
        if p.get("is_delayed") and p.get("status") != "보류"
    ]

    return render_template(
        "dashboard.html",
        summary=summary,
        projects=projects,
        delayed_projects=delayed_projects,
        approval_pending_projects=approval_pending_projects,
    )

@app.route("/kpi")
def kpi():
    today = now_kst().date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=7)

    projects = [
        enrich_project(project)
        for project in load_projects()
        if not project.get("is_deleted", False)
    ]

    all_history = []
    hold_history = []

    new_delay_count = 0
    stage_delay_count = {}
    stage_total_count = {}
    stage_stay_days = {}

    for project in projects:
        for stage in project["stages"]:
            stage_name = f'{stage["stage_order"]} {stage["stage_name"]}'
            stage_total_count[stage_name] = stage_total_count.get(stage_name, 0) + 1

            if stage.get("status") == "지연":
                stage_delay_count[stage_name] = stage_delay_count.get(stage_name, 0) + 1

            if stage.get("status") == "지연" and stage.get("delay_deadline"):
                deadline = parse_date(stage.get("delay_deadline"))
                if deadline:
                    delay_start = deadline + timedelta(days=1)
                    if week_start <= delay_start < week_end:
                        new_delay_count += 1

            planned = parse_date(stage.get("planned_date"))
            actual = parse_date(stage.get("actual_date"))

            if planned and actual:
                stay_days = max((actual - planned).days, 0)
                stage_stay_days.setdefault(stage_name, []).append(stay_days)

        for row in get_project_history(project["id"]):
            item = dict(row)
            item["project_code"] = project.get("code")
            item["project_name"] = project.get("name")
            all_history.append(item)

        for row in project.get("hold_history", []):
            item = dict(row)
            item["project_code"] = project.get("code")
            item["project_name"] = project.get("name")
            hold_history.append(item)

    def is_this_week_date(date_text):
        d = parse_date(str(date_text)[:10])
        return d and week_start <= d < week_end

    weekly_history = [
        row for row in all_history
        if is_this_week_date(row.get("changed_at"))
    ]

    planned_change_count = sum(
        1 for row in weekly_history
        if row.get("field_name") == "planned_date"
    )

    actual_input_count = sum(
        1 for row in weekly_history
        if row.get("field_name") == "actual_date" and row.get("new_value")
    )

    approval_count = 0
    for project in projects:
        for stage in project["stages"]:
            if is_this_week_date(stage.get("approval_date")):
                approval_count += 1

    hold_request_count = sum(
        1 for row in hold_history
        if row.get("type") == "보류신청" and is_this_week_date(row.get("at"))
    )

    recent_history = sorted(
        all_history,
        key=lambda x: x.get("changed_at", ""),
        reverse=True
    )[:30]

    stage_stay_rows = []
    for stage_name, days in stage_stay_days.items():
        if days:
            avg_days = round(sum(days) / len(days), 1)
            stage_stay_rows.append({
                "stage": stage_name,
                "avg_days": avg_days,
                "count": len(days),
            })

    stage_stay_rows = sorted(stage_stay_rows, key=lambda x: x["avg_days"], reverse=True)

    stage_delay_rate_rows = []
    for stage_name, total in stage_total_count.items():
        delayed = stage_delay_count.get(stage_name, 0)
        rate = round((delayed / total) * 100, 1) if total else 0
        if delayed > 0:
            stage_delay_rate_rows.append({
                "stage": stage_name,
                "delayed": delayed,
                "total": total,
                "rate": rate,
            })

    stage_delay_rate_rows = sorted(stage_delay_rate_rows, key=lambda x: x["rate"], reverse=True)

    project_ids_with_plan_change = set(
        row.get("project_code")
        for row in all_history
        if row.get("field_name") == "planned_date"
    )

    plan_change_rate = round(
        (len(project_ids_with_plan_change) / len(projects)) * 100,
        1
    ) if projects else 0

    return render_template(
        "kpi.html",
        week_start=week_start,
        week_end=week_end - timedelta(days=1),
        summary={
            "new_delay": new_delay_count,
            "planned_change": planned_change_count,
            "actual_input": actual_input_count,
            "approval": approval_count,
            "hold_request": hold_request_count,
        },
        stage_stay_rows=stage_stay_rows,
        stage_delay_rate_rows=stage_delay_rate_rows,
        plan_change_rate=plan_change_rate,
        recent_history=recent_history,
    )

@app.route("/projects")
def projects():
    # recompute_all_projects()
    filtered_projects, keyword, status, delay = get_filtered_projects()
    filtered_projects = sorted(filtered_projects, key=safe_code_sort)
    status_options = ["진행", "승인대기", "완료", "보류", "지연", "누락"]

    return render_template(
        "projects.html",
        projects=filtered_projects,
        keyword=keyword,
        status=status,
        delay=delay,
        status_options=status_options,
    )


@app.route("/projects/new")
def project_new():
    if not require_master():
        return redirect(url_for("projects"))
    return render_template("project_form.html", project=None)


@app.route("/projects/create", methods=["POST"])
def project_create():
    if not require_master():
        return redirect(url_for("projects"))

    projects = load_projects()

    code = request.form.get("code", "").strip()
    name = request.form.get("name", "").strip()
    customer = request.form.get("customer", "").strip()
    location = request.form.get("location", "").strip()
    order_date = request.form.get("order_date", "").strip()
    due_date = request.form.get("due_date", "").strip()
    pm_name = request.form.get("pm_name", "").strip()

    if not name:
        flash("프로젝트명은 필수입니다.")
        return redirect(url_for("project_new"))

    if not code:
        code = generate_project_code()

    new_id = get_next_project_id()

    new_project = {
        "id": new_id,
        "code": code,
        "name": name,
        "customer": customer,
        "location": location,
        "order_date": order_date or None,
        "due_date": due_date or None,
        "status": "진행",
        "pm_name": pm_name,
        "current_stage": STAGE_MASTER[0]["stage_name"],
        "current_stage_order": STAGE_MASTER[0]["stage_order"],
        "is_deleted": False,
        "is_hold": False,
        "hold_requested": False,
        "hold_request_by": "",
        "hold_request_reason": "",
        "hold_request_memo": "",
        "hold_request_at": None,
        "hold_reason": "",
        "hold_start_date": None,
        "hold_end_date": None,
        "hold_history": [],
        "release_requested": False,
        "release_request_by": "",
        "release_request_reason": "",
        "release_request_at": None,
    }

    projects.append(new_project)
    save_projects(projects)

    initial_stages = [
        {
            "stage_order": master["stage_order"],
            "stage_name": master["stage_name"],
            "assignee_name": get_fixed_assignee(master["stage_order"]),
            "planned_date": None,
            "actual_date": None,
            "approval_date": None,
            "note": "",
            "status": "",
            "is_not_applicable": False,
        }
        for master in STAGE_MASTER
    ]
    save_project_stages(new_id, initial_stages)

    save_project_teams(
        new_id,
        {
            "team_rows": [
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            ]
        },
    )

    save_project_history(new_id, [])

    recompute_project(new_id)
    flash("프로젝트가 등록되었습니다.")
    return redirect(url_for("project_detail", project_id=new_id))


@app.route("/projects/<int:project_id>")
def project_detail(project_id: int):
    project = find_project(project_id)
    if not project:
        abort(404)

    recompute_project(project_id)
    project = find_project(project_id)

    enriched_project = enrich_project(project)
    team_data = get_project_team(project_id)

    return render_template(
        "project_detail.html",
        project=enriched_project,
        stages=enriched_project["stages"],
        team_rows=team_data.get("team_rows", []),
    )


@app.route("/projects/<int:project_id>/edit")
def project_edit(project_id: int):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    project = find_project(project_id)
    if not project:
        abort(404)

    return render_template("project_form.html", project=project)


@app.route("/projects/<int:project_id>/edit", methods=["POST"])
def project_edit_submit(project_id: int):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id and not p.get("is_deleted", False)), None)
    if not project:
        abort(404)

    code = request.form.get("code", "").strip()
    name = request.form.get("name", "").strip()
    customer = request.form.get("customer", "").strip()
    location = request.form.get("location", "").strip()
    order_date = request.form.get("order_date", "").strip()
    due_date = request.form.get("due_date", "").strip()
    pm_name = request.form.get("pm_name", "").strip()

    if not name:
        flash("프로젝트명은 필수입니다.")
        return redirect(url_for("project_edit", project_id=project_id))

    project["code"] = code or project["code"]
    project["name"] = name
    project["customer"] = customer
    project["location"] = location
    project["order_date"] = order_date or None
    project["due_date"] = due_date or None
    project["pm_name"] = pm_name

    save_projects(projects)
    recompute_project(project_id)

    flash("프로젝트 기본정보가 수정되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/update", methods=["POST"])
def update_project(project_id):
    project = find_project(project_id)
    if not project:
        abort(404)

    existing_stages = load_project_stages(project_id)
    existing_stage_map = {
        stage["stage_order"]: stage
        for stage in existing_stages
    }

    planned_map = {}
    actual_map = {}
    note_map = {}
    na_map = {}

    for master in STAGE_MASTER:
        key = master["stage_order"]
        planned_map[key] = request.form.get(f"planned_{key}", "").strip() or None
        actual_map[key] = request.form.get(f"actual_{key}", "").strip() or None
        note_map[key] = request.form.get(f"note_{key}", "").strip()
        na_map[key] = request.form.get(f"not_applicable_{key}") == "Y"

    auto_plan_3 = add_days(actual_map.get("2"), 7)

    if not planned_map.get("3"):
        planned_map["3"] = auto_plan_3

    if not planned_map.get("4"):
        planned_map["4"] = add_days(planned_map.get("3"), 7)

    if not planned_map.get("5"):
        planned_map["5"] = add_days(planned_map.get("3"), 7)

    updated_list = []

    for master in STAGE_MASTER:
        key = master["stage_order"]
        assignee_name = get_fixed_assignee(key)
        existing_stage = existing_stage_map.get(key, {})

        planned_date = planned_map.get(key)
        actual_date = actual_map.get(key)
        note = note_map.get(key, "")
        is_not_applicable = na_map.get(key, False)

        approval_date = existing_stage.get("approval_date")

        planned_date = planned_date or None
        actual_date = actual_date or None
        approval_date = approval_date or None

        old_planned_date = existing_stage.get("planned_date")
        old_actual_date = existing_stage.get("actual_date")

        changed_by_planned = request.form.get(f"changed_by_planned_{key}", "").strip()
        change_reason_planned = request.form.get(f"change_reason_planned_{key}", "").strip()

        changed_by_actual = request.form.get(f"changed_by_actual_{key}", "").strip()
        change_reason_actual = request.form.get(f"change_reason_actual_{key}", "").strip()

        if old_planned_date != planned_date:
            if not old_planned_date and planned_date:
                add_stage_change_history(
                    project_id=project_id,
                    stage_order=key,
                    field_name="planned_date",
                    field_label="계획일",
                    old_value="",
                    new_value=planned_date,
                    changed_by="SYSTEM",
                    change_reason="최초 계획일 입력",
                )
            elif changed_by_planned and change_reason_planned:
                add_stage_change_history(
                    project_id=project_id,
                    stage_order=key,
                    field_name="planned_date",
                    field_label="계획일",
                    old_value=old_planned_date,
                    new_value=planned_date,
                    changed_by=changed_by_planned,
                    change_reason=change_reason_planned,
                )

        if old_actual_date != actual_date:
            if not old_actual_date and actual_date:
                add_stage_change_history(
                    project_id=project_id,
                    stage_order=key,
                    field_name="actual_date",
                    field_label="실적일",
                    old_value="",
                    new_value=actual_date,
                    changed_by="SYSTEM",
                    change_reason="최초 실적일 입력",
                )
            elif changed_by_actual and change_reason_actual:
                add_stage_change_history(
                    project_id=project_id,
                    stage_order=key,
                    field_name="actual_date",
                    field_label="실적일",
                    old_value=old_actual_date,
                    new_value=actual_date,
                    changed_by=changed_by_actual,
                    change_reason=change_reason_actual,
                )

            approval_date = None

        updated_list.append(
            {
                "stage_order": key,
                "stage_name": master["stage_name"],
                "assignee_name": assignee_name,
                "planned_date": planned_date,
                "actual_date": actual_date,
                "approval_date": approval_date,
                "note": note,
                "status": "",
                "is_not_applicable": is_not_applicable,
            }
        )

    save_project_stages(project_id, updated_list)

    pm_list = request.form.getlist("team_pm[]")
    design_list = request.form.getlist("team_design[]")
    machine_list = request.form.getlist("team_machine[]")
    control_list = request.form.getlist("team_control[]")
    sales_list = request.form.getlist("team_sales[]")

    team_rows = normalize_team_rows(
        pm_list,
        design_list,
        machine_list,
        control_list,
        sales_list,
    )

    save_project_teams(
        project_id,
        {
            "team_rows": team_rows
        }
    )

    recompute_project(project_id)
    flash("프로젝트 상세가 수정되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/request-hold", methods=["POST"])
def request_hold(project_id):
    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id), None)

    if not project:
        abort(404)

    request_by = request.form.get("hold_request_by", "").strip()
    reason = request.form.get("hold_request_reason", "").strip()
    memo = request.form.get("hold_request_memo", "").strip()
    requested_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    project["hold_requested"] = True
    project["hold_request_by"] = request_by
    project["hold_request_reason"] = reason
    project["hold_request_memo"] = memo
    project["hold_request_at"] = requested_at

    history = project.get("hold_history", [])
    history.append({
        "type": "보류신청",
        "at": requested_at,
        "by": request_by,
        "reason": reason,
        "memo": memo,
    })
    project["hold_history"] = history

    save_projects(projects)

    flash("보류 신청이 등록되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/request-release", methods=["POST"])
def request_release(project_id):
    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id), None)

    if not project:
        abort(404)

    requested_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    request_by = request.form.get("release_request_by", "").strip()
    reason = request.form.get("release_request_reason", "").strip()

    project["release_requested"] = True
    project["release_request_by"] = request_by
    project["release_request_reason"] = reason
    project["release_request_at"] = requested_at

    history = project.get("hold_history", [])
    history.append({
        "type": "보류해제신청",
        "at": requested_at,
        "by": request_by,
        "reason": reason,
        "memo": "",
    })
    project["hold_history"] = history

    save_projects(projects)

    flash("보류 해제 요청이 등록되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/hold", methods=["POST"])
def set_hold(project_id):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id), None)

    if not project:
        abort(404)

    approved_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    reason = project.get("hold_request_reason", "") or request.form.get("hold_reason", "").strip()
    memo = project.get("hold_request_memo", "")

    project["is_hold"] = True
    project["hold_requested"] = False
    project["hold_reason"] = reason
    project["hold_start_date"] = approved_at
    project["hold_end_date"] = None

    history = project.get("hold_history", [])
    history.append({
        "type": "보류승인",
        "at": approved_at,
        "by": project.get("hold_request_by", ""),
        "reason": reason,
        "memo": memo,
    })
    project["hold_history"] = history

    save_projects(projects)
    recompute_project(project_id)

    flash("프로젝트가 보류 처리되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/release-hold", methods=["POST"])
def release_hold(project_id):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    projects = load_projects()
    project = next((p for p in projects if p["id"] == project_id), None)

    if not project:
        abort(404)

    released_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    project["is_hold"] = False
    project["hold_requested"] = False
    project["hold_end_date"] = released_at
    project["release_requested"] = False
    
    history = project.get("hold_history", [])
    history.append({
        "type": "보류해제",
        "at": released_at,
        "by": "",
        "reason": "보류 해제",
        "memo": "",
    })
    project["hold_history"] = history

    save_projects(projects)
    recompute_project(project_id)

    flash("보류가 해제되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/delete", methods=["POST"])
def project_delete(project_id: int):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    projects = load_projects()
    target = next((p for p in projects if p["id"] == project_id), None)

    if not target:
        abort(404)

    target["is_deleted"] = True
    save_projects(projects)

    flash("프로젝트가 삭제되었습니다.")
    return redirect(url_for("projects"))


@app.route("/projects/<int:project_id>/approve/<stage_order>", methods=["POST"])
def approve_stage(project_id: int, stage_order: str):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    project = find_project(project_id)
    if not project:
        abort(404)

    stages = load_project_stages(project_id)
    target = next((stage for stage in stages if stage["stage_order"] == stage_order), None)

    if not target:
        master = next((m for m in STAGE_MASTER if m["stage_order"] == stage_order), None)
        if not master:
            abort(404)

        target = {
            "stage_order": stage_order,
            "stage_name": master["stage_name"],
            "assignee_name": get_fixed_assignee(stage_order),
            "planned_date": None,
            "actual_date": None,
            "approval_date": None,
            "note": "",
            "status": "",
            "is_not_applicable": False,
        }
        stages.append(target)

    if not target.get("actual_date"):
        flash("실적일을 먼저 저장한 뒤 승인하세요.")
        return redirect(url_for("project_detail", project_id=project_id))

    if target.get("actual_date") and not target.get("approval_date"):
        target["approval_date"] = now_kst().strftime("%Y-%m-%d")

        add_stage_change_history(
            project_id=project_id,
            stage_order=stage_order,
            field_name="approval",
            field_label="승인",
            old_value="미승인",
            new_value=target["approval_date"],
            changed_by="품질팀",
            change_reason="단계 승인",
        )

        save_project_stages(project_id, stages)
        flash("승인 처리되었습니다.")

    recompute_project(project_id)
    return redirect(url_for("project_detail", project_id=project_id))


@app.route("/projects/<int:project_id>/approve-cancel/<stage_order>", methods=["POST"])
def cancel_approve_stage(project_id: int, stage_order: str):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    project = find_project(project_id)
    if not project:
        abort(404)

    stages = load_project_stages(project_id)
    target = next((stage for stage in stages if stage["stage_order"] == stage_order), None)

    if not target:
        abort(404)

    if target.get("approval_date"):
        old_approval_date = target.get("approval_date")
        target["approval_date"] = None

        add_stage_change_history(
            project_id=project_id,
            stage_order=stage_order,
            field_name="approval",
            field_label="승인취소",
            old_value=old_approval_date,
            new_value="미승인",
            changed_by="품질팀",
            change_reason="승인 취소",
        )

        save_project_stages(project_id, stages)

    recompute_project(project_id)
    flash("승인이 취소되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/history/<stage_order>")
def project_stage_history(project_id: int, stage_order: str):
    project = find_project(project_id)
    if not project:
        abort(404)

    rows = get_stage_history_rows(project_id, stage_order)

    return jsonify({
        "ok": True,
        "items": rows
    })

@app.route("/projects/<int:project_id>/history")
def project_all_history(project_id: int):
    project = find_project(project_id)
    if not project:
        abort(404)

    rows = get_project_history(project_id)
    rows = sorted(rows, key=lambda x: x.get("changed_at", ""), reverse=True)

    return jsonify({
        "ok": True,
        "items": rows
    })

if __name__ == "__main__":
    app.run(debug=True)