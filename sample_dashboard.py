"""
University Data Warehouse - Enhanced Interactive Dashboard
Created by Lord Ahmed and Alfred 🧠
Usage:
  python enhanced_dashboard.py
Then open http://127.0.0.1:8050 in your browser.
"""

import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output

# --- DB credentials ---
MYSQL_USER = "root"
MYSQL_PASS = "admin"
MYSQL_HOST = "localhost"
MYSQL_DB = "dwh_university"
# -----------------------

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}/{MYSQL_DB}")

# --- Initialize App ---
app = Dash(__name__)
app.title = "🎓 University Data Warehouse - Enhanced Dashboard"

# --- Global Theme ---
BACKGROUND = "linear-gradient(135deg, #eef2f3, #cfd9df)"
CARD_STYLE = {
    "backgroundColor": "white",
    "padding": "25px",
    "borderRadius": "18px",
    "boxShadow": "0 4px 12px rgba(0,0,0,0.1)",
    "marginBottom": "25px",
}

# --- Layout ---
app.layout = html.Div(
    style={
        "fontFamily": "Segoe UI, sans-serif",
        "padding": "40px",
        "backgroundImage": BACKGROUND,
        "minHeight": "100vh",
    },
    children=[
        html.H1(
            "🎓 University Data Warehouse - Enhanced Dashboard",
            style={
                "textAlign": "center",
                "color": "#1a237e",
                "marginBottom": "20px",
                "fontWeight": "700",
            },
        ),
        html.P(
            "Comprehensive academic, HR, and financial analytics board crafted by NAS.",
            style={"textAlign": "center", "color": "#37474f", "marginBottom": "30px"},
        ),

        html.Div(
            [
                html.Label(
                    "📊 Select Insights to Display:",
                    style={"fontSize": "18px", "fontWeight": "600", "color": "#263238"},
                ),
                dcc.Dropdown(
                    id="insight_selector",
                    options=[
                        {"label": "📘 Academic Performance Overview", "value": "academic_overview"},
                        {"label": "🎯 Student Demographics", "value": "student_demo"},
                        {"label": "💼 HR Analytics", "value": "hr_analytics"},
                        {"label": "💰 Financial Overview", "value": "finance_overview"},
                        {"label": "📈 Department Performance", "value": "dept_performance"},
                        {"label": "👨‍🏫 Instructor Analysis", "value": "instructor_analysis"},
                    ],
                    value=[],
                    multi=True,
                    placeholder="Select one or more insights...",
                    style={"width": "80%", "margin": "15px auto"},
                ),
            ],
            style={"textAlign": "center", "marginBottom": "35px"},
        ),

        html.Div(id="graphs_container", style={"maxWidth": "1400px", "margin": "auto"}),
    ],
)


# --- Callbacks ---
@app.callback(
    Output("graphs_container", "children"),
    Input("insight_selector", "value"),
)
def update_graphs(selected):
    graphs = []
    if not selected:
        return html.Div(
            "Please select one or more insights from the dropdown above.",
            style={
                "textAlign": "center",
                "fontSize": "18px",
                "color": "#546e7a",
                "marginTop": "40px",
            },
        )

    # --- Academic Performance Overview ---
    if "academic_overview" in selected:
        # 1. Grade Distribution
        q1 = """
        SELECT grade, COUNT(*) as count
        FROM fact_academics 
        WHERE grade IS NOT NULL
        GROUP BY grade
        ORDER BY grade;
        """
        df1 = pd.read_sql(q1, engine)
        fig1 = px.histogram(df1, x="grade", y="count", nbins=20, 
                           title="📊 Grade Distribution",
                           color_discrete_sequence=['#636EFA'])
        
        # 2. Average Grade by Semester
        q2 = """
        SELECT d.semester, AVG(f.grade) AS avg_grade, COUNT(*) as enrollment
        FROM fact_academics f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.grade IS NOT NULL
        GROUP BY d.semester
        ORDER BY d.semester;
        """
        df2 = pd.read_sql(q2, engine)
        fig2 = px.line(df2, x="semester", y="avg_grade", 
                      title="📈 Average Grade Trend by Semester",
                      markers=True)
        fig2.update_traces(line=dict(width=3))
        
        # Create subplot for academic overview
        academic_fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Grade Distribution', 'Average Grade by Semester')
        )
        
        academic_fig.add_trace(go.Bar(x=df1['grade'], y=df1['count'], name='Grade Distribution'), 1, 1)
        academic_fig.add_trace(go.Scatter(x=df2['semester'], y=df2['avg_grade'], 
                                        mode='lines+markers', name='Avg Grade'), 1, 2)
        
        academic_fig.update_layout(height=400, title_text="Academic Performance Overview", 
                                 showlegend=False)
        
        graphs.append(html.Div(dcc.Graph(figure=academic_fig), style=CARD_STYLE))

    # --- Student Demographics ---
    if "student_demo" in selected:
        # 1. Gender Distribution
        q3 = """
        SELECT gender, COUNT(*) as count
        FROM dim_student
        GROUP BY gender;
        """
        df3 = pd.read_sql(q3, engine)
        fig3 = px.pie(df3, names="gender", values="count", 
                     title="👥 Student Gender Distribution", hole=0.4)
        
        # 2. Students by Program
        q4 = """
        SELECT program, COUNT(*) as student_count
        FROM dim_student
        GROUP BY program
        ORDER BY student_count DESC
        LIMIT 10;
        """
        df4 = pd.read_sql(q4, engine)
        fig4 = px.bar(df4, x="program", y="student_count",
                     title="🎓 Top 10 Programs by Student Count",
                     color="student_count")
        
        graphs.append(html.Div(dcc.Graph(figure=fig3), style=CARD_STYLE))
        graphs.append(html.Div(dcc.Graph(figure=fig4), style=CARD_STYLE))

    # --- HR Analytics ---
    if "hr_analytics" in selected:
        # 1. Salary Distribution by Department
        q5 = """
        SELECT d.department_name, AVG(e.salary) as avg_salary, COUNT(*) as employee_count
        FROM dim_employee e
        JOIN dim_department d ON e.department_id = d.department_id
        GROUP BY d.department_name
        ORDER BY avg_salary DESC;
        """
        df5 = pd.read_sql(q5, engine)
        fig5 = px.bar(df5, x="department_name", y="avg_salary",
                     title="💼 Average Salary by Department",
                     color="avg_salary")
        
        # 2. Employee Count by Job Title
        q6 = """
        SELECT job_title, COUNT(*) as count
        FROM dim_employee
        GROUP BY job_title
        ORDER BY count DESC
        LIMIT 8;
        """
        df6 = pd.read_sql(q6, engine)
        fig6 = px.pie(df6, names="job_title", values="count",
                     title="👨‍💼 Employee Distribution by Job Title")
        
        graphs.append(html.Div(dcc.Graph(figure=fig5), style=CARD_STYLE))
        graphs.append(html.Div(dcc.Graph(figure=fig6), style=CARD_STYLE))

    # --- Financial Overview ---
    if "finance_overview" in selected:
        # 1. Revenue vs Expense by Month
        q7 = """
        SELECT d.month, d.year, 
               SUM(CASE WHEN f.transaction_type = 'Revenue' THEN f.amount ELSE 0 END) as revenue,
               SUM(CASE WHEN f.transaction_type = 'Expense' THEN f.amount ELSE 0 END) as expense
        FROM fact_finance f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
        LIMIT 12;
        """
        df7 = pd.read_sql(q7, engine)
        df7['period'] = df7['month'].astype(str) + '-' + df7['year'].astype(str)
        
        fig7 = go.Figure()
        fig7.add_trace(go.Bar(name='Revenue', x=df7['period'], y=df7['revenue']))
        fig7.add_trace(go.Bar(name='Expense', x=df7['period'], y=df7['expense']))
        fig7.update_layout(barmode='group', title='💰 Revenue vs Expense by Month')
        
        # 2. Transaction Types
        q8 = """
        SELECT transaction_type, SUM(amount) as total_amount
        FROM fact_finance
        GROUP BY transaction_type;
        """
        df8 = pd.read_sql(q8, engine)
        fig8 = px.pie(df8, names="transaction_type", values="total_amount",
                     title="💳 Financial Transaction Distribution")
        
        graphs.append(html.Div(dcc.Graph(figure=fig7), style=CARD_STYLE))
        graphs.append(html.Div(dcc.Graph(figure=fig8), style=CARD_STYLE))

    # --- Department Performance ---
    if "dept_performance" in selected:
        # 1. Department Budget vs Performance
        q9 = """
        SELECT d.department_name, d.budget, 
               AVG(f.performance_rating) as avg_performance,
               COUNT(DISTINCT e.employee_id) as employee_count
        FROM dim_department d
        LEFT JOIN dim_employee e ON d.department_id = e.department_id
        LEFT JOIN fact_hr_metrics f ON e.employee_id = f.employee_id
        GROUP BY d.department_name, d.budget
        HAVING avg_performance IS NOT NULL;
        """
        df9 = pd.read_sql(q9, engine)
        fig9 = px.scatter(df9, x="budget", y="avg_performance", size="employee_count",
                         color="department_name", hover_name="department_name",
                         title="🏢 Department Budget vs Performance Rating",
                         size_max=60)
        
        graphs.append(html.Div(dcc.Graph(figure=fig9), style=CARD_STYLE))

    # --- Instructor Analysis ---
    if "instructor_analysis" in selected:
        # 1. Top Instructors by Average Grade
        q10 = """
        SELECT CONCAT(e.first_name, ' ', e.last_name) AS instructor_name,
               e.job_title, d.department_name,
               AVG(f.grade) AS avg_grade,
               COUNT(DISTINCT fa.student_id) as students_taught
        FROM dim_employee e
        JOIN fact_academics fa ON e.employee_id = fa.employee_id
        JOIN dim_department d ON e.department_id = d.department_id
        WHERE e.job_title LIKE '%Professor%' OR e.job_title LIKE '%Instructor%'
        GROUP BY instructor_name, e.job_title, d.department_name
        HAVING avg_grade IS NOT NULL
        ORDER BY avg_grade DESC
        LIMIT 15;
        """
        df10 = pd.read_sql(q10, engine)
        fig10 = px.bar(df10, x="instructor_name", y="avg_grade", 
                      color="department_name",
                      title="👨‍🏫 Top Instructors by Average Grade",
                      hover_data=["job_title", "students_taught"])
        fig10.update_layout(xaxis_tickangle=-45)
        
        graphs.append(html.Div(dcc.Graph(figure=fig10), style=CARD_STYLE))

    return graphs


# --- Run the App ---
if __name__ == "__main__":
    app.run(debug=True, port=8050)