import os
from flask import Flask, render_template, abort, request, redirect, url_for, flash, jsonify, session
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "260407")

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

PROJECTS = [
    {
        "id": 1,
        "code": "25037",
        "name": "화성)세타 3C라인 세타3 T-GDI 기종추가 갠트리 및 자동화 개조",
        "customer": "KMC",
        "location": "화성",
        "order_date": "2025-04-23",
        "due_date": "2026-03-08",
        "status": "진행",
        "pm_name": "김지명",
        "current_stage": "CHECK SHEET",
        "current_stage_order": "7-1",
        "is_delayed": False,
        "is_deleted": False,
    },
    {
        "id": 2,
        "code": "25056",
        "name": "코넥)TESLA 3DUR ITEM 이관건",
        "customer": "코넥",
        "location": "울산",
        "order_date": "2025-05-10",
        "due_date": "2026-02-20",
        "pm_name": "김지명",
        "current_stage": "업체선정",
        "current_stage_order": "8",
        "is_delayed": False,
        "is_deleted": False,
    },
]

PROJECT_STAGES = {
    1: [
        {
            "stage_order": "1",
            "stage_name": "작업지시서",
            "assignee_name": "영업팀",
            "planned_date": "2025-08-07",
            "actual_date": "2025-08-07",
            "approval_date": "2025-08-07",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "2",
            "stage_name": "PM지정",
            "assignee_name": "생관팀",
            "planned_date": "2025-08-14",
            "actual_date": "2025-08-07",
            "approval_date": "2025-08-07",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "3",
            "stage_name": "설명회",
            "assignee_name": "영업팀",
            "planned_date": "2025-08-17",
            "actual_date": "2025-08-08",
            "approval_date": "2025-08-08",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "4",
            "stage_name": "팀구성",
            "assignee_name": "생관팀",
            "planned_date": "2025-08-19",
            "actual_date": "2025-08-07",
            "approval_date": "2025-08-07",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "5",
            "stage_name": "착수보고서작성",
            "assignee_name": "PM",
            "planned_date": "2025-08-21",
            "actual_date": "2025-08-18",
            "approval_date": "2025-08-18",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "6",
            "stage_name": "승인협의",
            "assignee_name": "PM",
            "planned_date": None,
            "actual_date": "2025-08-19",
            "approval_date": "2025-08-19",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "7",
            "stage_name": "점검회의",
            "assignee_name": "PM",
            "planned_date": None,
            "actual_date": "2025-09-01",
            "approval_date": "2025-09-01",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "7-1",
            "stage_name": "CHECK SHEET",
            "assignee_name": "설계팀",
            "planned_date": None,
            "actual_date": "2025-08-25",
            "approval_date": "2025-08-25",
            "note": "설계업무 / 외주설계",
            "status": "진행",
            "is_not_applicable": False,
        },
        {
            "stage_order": "8",
            "stage_name": "업체선정",
            "assignee_name": "구매팀",
            "planned_date": None,
            "actual_date": "2025-09-29",
            "approval_date": "2025-09-29",
            "note": "조립:CMT, 두원 / 설치:CMT / 제어:나라 / 프로그램:국동/유승",
            "is_not_applicable": False,
        },
    ],
    2: [
        {
            "stage_order": "1",
            "stage_name": "작업지시서",
            "assignee_name": "영업팀",
            "planned_date": "2025-12-03",
            "actual_date": "2025-12-03",
            "approval_date": "2025-12-03",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "2",
            "stage_name": "PM지정",
            "assignee_name": "생관팀",
            "planned_date": "2025-12-10",
            "actual_date": "2025-12-11",
            "approval_date": "2025-12-11",
            "note": "-",
            "status": "완료",
            "is_not_applicable": False,
        },
        {
            "stage_order": "8",
            "stage_name": "업체선정",
            "assignee_name": "구매팀",
            "planned_date": "2026-01-28",
            "actual_date": None,
            "approval_date": None,
            "note": "협력사 검토 중",
            "is_not_applicable": False,
        },
    ],
}

PROJECT_TEAMS = {
    1: {
        "team_rows": [
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
        ],
    },
    2: {
        "team_rows": [
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
        ],
    },
}

STAGE_CHANGE_HISTORY = {
    1: [
        # 예시
        # {
        #     "stage_order": "8",
        #     "field_name": "planned_date",
        #     "field_label": "계획일",
        #     "old_value": "2026-01-20",
        #     "new_value": "2026-01-28",
        #     "changed_by": "이정기",
        #     "change_reason": "업체 검토 일정 변경",
        #     "changed_at": "2026-03-30 11:20:00",
        # }
    ],
    2: [],
}

def find_project(project_id: int, include_deleted: bool = False):
    for project in PROJECTS:
        if project["id"] == project_id:
            if not include_deleted and project.get("is_deleted", False):
                return None
            return project
    return None

def get_next_project_id():
    if not PROJECTS:
        return 1
    return max(project["id"] for project in PROJECTS) + 1


def generate_project_code():
    year_prefix = datetime.today().strftime("%y")
    same_year_codes = []

    for project in PROJECTS:
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

def add_days(date_str, days):
    d = parse_date(date_str)
    if not d:
        return None
    return (d + timedelta(days=days)).strftime("%Y-%m-%d")

def get_project_history(project_id: int):
    return STAGE_CHANGE_HISTORY.setdefault(project_id, [])


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
            "changed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


def get_stage_history_rows(project_id: int, stage_order: str):
    history_rows = get_project_history(project_id)
    rows = [
        row for row in history_rows
        if str(row.get("stage_order")) == str(stage_order)
        and row.get("field_name") in ["planned_date", "actual_date"]
    ]
    rows.sort(key=lambda x: x["changed_at"], reverse=True)
    return rows

def find_stage_in_project(project_id: int, stage_order: str):
    stages = PROJECT_STAGES.setdefault(project_id, [])
    return next((stage for stage in stages if stage["stage_order"] == stage_order), None)

def get_project_team(project_id: int):
    return PROJECT_TEAMS.setdefault(
        project_id,
        {
            "team_rows": [
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
                {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            ],
        },
    )


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
    actual = parse_date(actual_date)
    approval = parse_date(approval_date)
    today = datetime.today().date()

    # 1. 실적이 있으면 누락보다 승인/완료를 우선 본다
    if actual and approval:
        return "완료"

    if actual and not approval:
        return "승인대기"

    # 2. 실적이 없을 때만 누락 여부를 본다
    if has_missing_required_fields(
        stage_order,
        assignee_name,
        planned_date,
        actual_date,
        approval_date,
        note,
    ):
        return "누락"

    # 3. 계획일만 있고 아직 실적이 없으면 진행/지연
    if planned and not actual:
        if planned < today:
            return "지연"
        return "진행"

    return "누락"


def merge_stages(project_id: int):
    saved_map = {stage["stage_order"]: stage for stage in PROJECT_STAGES.get(project_id, [])}
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

        # ===== 업무 규칙 =====
        # 6~9단계는 5번(착수보고서작성) 완료 전까지는 미착수로 본다.
        if master["stage_order"] in ["6", "7", "7-1", "8", "9"] and not is_not_applicable:
            stage5 = next((s for s in merged if s["stage_order"] == "5"), None)
            stage5_completed = stage5 and stage5.get("status") == "완료"

            if not stage5_completed:
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
                "status": status,
                "is_not_applicable": is_not_applicable,
            }
        )

    return merged


def recompute_project(project_id):
    project = find_project(project_id)
    if not project:
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

    if any(s["status"] == "승인대기" for s in merged):
        project["status"] = "승인대기"
    elif all(s["status"] in ["완료", "해당없음"] for s in merged):
        project["status"] = "완료"
    elif any(s["status"] == "진행" for s in merged):
        project["status"] = "진행"
    else:
        project["status"] = "진행"

def recompute_all_projects():
    for project in PROJECTS:
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
    completed_count = sum(
    1 for stage in stages
    if stage["status"] in ["완료", "해당없음"]
    )
    progress_text = f"{completed_count}/{ACTUAL_STAGE_COUNT}"
    progress_percent = int((completed_count / ACTUAL_STAGE_COUNT) * 100)
    current_stage_display = f'{project["current_stage_order"]} {project["current_stage"]}'

    enriched = dict(project)
    enriched["completed_count"] = completed_count
    enriched["progress_text"] = progress_text
    enriched["progress_percent"] = progress_percent
    enriched["progress_color"] = get_progress_color(progress_percent, project["is_delayed"])
    enriched["current_stage_display"] = current_stage_display
    enriched["is_missing"] = project.get("is_missing", False)
    enriched["stage_mini_view"] = build_stage_mini_view(stages)
    enriched["stages"] = stages
    return enriched


def get_filtered_projects():
    keyword = request.args.get("keyword", "").strip().lower()
    status = request.args.get("status", "").strip()
    delay = request.args.get("delay", "").strip()

    enriched_projects = [
        enrich_project(project)
        for project in PROJECTS
        if not project.get("is_deleted", False)
    ]
    filtered = []

    for project in enriched_projects:
        searchable = " ".join(
            [
                project["code"],
                project["name"],
                project["customer"],
                project["location"],
                project["pm_name"],
                project["current_stage"],
                project["current_stage_order"],
            ]
        ).lower()

        if keyword and keyword not in searchable:
            continue

        if status == "누락" and not project.get("is_missing", False):
            continue
        elif status == "지연" and not project.get("is_delayed", False):
            continue
        elif status and status not in ["누락", "지연"] and project["status"] != status:
            continue

        if delay == "Y" and not project["is_delayed"]:
            continue
        if delay == "N" and project["is_delayed"]:
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
    recompute_all_projects()
    projects = [
        enrich_project(project)
        for project in PROJECTS
        if not project.get("is_deleted", False)
    ]

    summary = {
        "total": len(projects),
        "in_progress": sum(1 for p in projects if p["status"] == "진행"),
        "approval_pending": sum(
            1 for p in projects
            if any(stage["status"] == "승인대기" for stage in merge_stages(p["id"]))
        ),
        "completed": sum(1 for p in projects if p["status"] == "완료"),
        "missing": sum(1 for p in projects if p.get("is_missing")),
        "delayed": sum(1 for p in projects if p["is_delayed"]),
    }

    delayed_projects = [p for p in projects if p["is_delayed"]]
    approval_pending_projects = []

    for p in projects:
        stages = merge_stages(p["id"])
        if any(stage["status"] == "승인대기" for stage in stages):
            approval_pending_projects.append(p)

    return render_template(
        "dashboard.html",
        summary=summary,
        projects=projects,
        delayed_projects=delayed_projects,
        approval_pending_projects=approval_pending_projects,
    )


@app.route("/projects")
def projects():
    recompute_all_projects()
    filtered_projects, keyword, status, delay = get_filtered_projects()
    status_options = ["진행", "승인대기", "완료", "지연", "누락"]

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
        "is_delayed": False,
        "is_deleted": False,
    }

    PROJECTS.append(new_project)

    PROJECT_STAGES[new_id] = [
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

    PROJECT_TEAMS[new_id] = {
        "team_rows": [
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
            {"pm": "", "design": "", "machine": "", "control": "", "sales": ""},
        ]
    }

    STAGE_CHANGE_HISTORY[new_id] = []

    recompute_project(new_id)
    flash("프로젝트가 등록되었습니다.")
    return redirect(url_for("project_detail", project_id=new_id))


@app.route("/projects/<int:project_id>")
def project_detail(project_id: int):
    project = find_project(project_id)
    if not project:
        abort(404)

    recompute_project(project_id)

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

    project = find_project(project_id)
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

    recompute_project(project_id)
    flash("프로젝트 기본정보가 수정되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/update", methods=["POST"])
def update_project(project_id):
    project = find_project(project_id)
    if not project:
        abort(404)

    existing_stage_map = {
        stage["stage_order"]: stage
        for stage in PROJECT_STAGES.get(project_id, [])
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

    # ===== 불변 규칙: 계획일 자동 산정 =====
    planned_map["3"] = add_days(actual_map.get("2"), 7)
    planned_map["4"] = add_days(planned_map.get("3"), 7)
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
            if changed_by_planned and change_reason_planned:
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
            if changed_by_actual and change_reason_actual:
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

        if old_actual_date != actual_date:
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

    PROJECT_STAGES[project_id] = updated_list

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

    PROJECT_TEAMS[project_id] = {
        "team_rows": team_rows
    }

    recompute_project(project_id)
    flash("프로젝트 상세가 수정되었습니다.")
    return redirect(url_for("project_detail", project_id=project_id))

@app.route("/projects/<int:project_id>/delete", methods=["POST"])
def project_delete(project_id: int):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    project = find_project(project_id)
    if not project:
        abort(404)

    project["is_deleted"] = True
    flash("프로젝트가 삭제되었습니다.")
    return redirect(url_for("projects"))

@app.route("/projects/<int:project_id>/approve/<stage_order>", methods=["POST"])
def approve_stage(project_id: int, stage_order: str):
    if not require_master():
        return redirect(url_for("project_detail", project_id=project_id))

    project = find_project(project_id)
    if not project:
        abort(404)

    stages = PROJECT_STAGES.setdefault(project_id, [])
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
        target["approval_date"] = datetime.today().strftime("%Y-%m-%d")
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

    stages = PROJECT_STAGES.setdefault(project_id, [])
    target = next((stage for stage in stages if stage["stage_order"] == stage_order), None)

    if not target:
        abort(404)

    if target.get("approval_date"):
        target["approval_date"] = None

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

if __name__ == "__main__":
    recompute_all_projects()
    app.run(debug=True)
