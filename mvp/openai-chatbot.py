import openai
# from dotenv import load_dotenv  # Azure App Service에서는 필요 없으므로 주석 처리
import os
import streamlit as st
from openai import AzureOpenAI, RateLimitError
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import json
import time
import logging
from typing import Optional, Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('investment_ai.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 환경 변수 로드 (Azure App Service는 OS 환경 변수를 직접 읽음)
# load_dotenv()

# 데이터베이스 연결 풀 생성 (전역 변수로 관리)
@st.cache_resource
def get_db_engine():
    """데이터베이스 연결 풀을 생성합니다."""
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            logger.error("데이터베이스 연결 정보가 설정되지 않았습니다.")
            return None
        
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,  # 1시간마다 연결 재생성
            echo=False  # SQL 로그 출력 (개발시에만 True)
        )
        logger.info("데이터베이스 연결 풀이 생성되었습니다.")
        return engine
    except Exception as e:
        logger.error(f"데이터베이스 연결 풀 생성 실패: {e}")
        return None

# AzureOpenAI 클라이언트 인스턴스 생성
@st.cache_resource
def get_openai_client():
    """OpenAI 클라이언트를 생성합니다."""
    try:
        client = AzureOpenAI(
            azure_endpoint=os.getenv('AZURE_ENDPOINT'),
            api_key=os.getenv('AZURE_API_KEY'),
            api_version=os.getenv('AZURE_API_VERSION')
        )
        logger.info("OpenAI 클라이언트가 생성되었습니다.")
        return client
    except Exception as e:
        logger.error(f"OpenAI 클라이언트 생성 실패: {e}")
        return None

# 전역적으로 클라이언트 인스턴스 생성
client = get_openai_client()

def get_table_schema() -> Dict[str, str]:
    """테이블의 컬럼 정보를 조회합니다."""
    engine = get_db_engine()
    if not engine:
        return {}
    
    try:
        schema_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'invest_excel'
            ORDER BY ordinal_position
        """)
        
        with engine.connect() as conn:
            result = conn.execute(schema_query)
            schema = {row[0]: row[1] for row in result}
            logger.info(f"테이블 스키마 조회 완료: {list(schema.keys())}")
            return schema
            
    except Exception as e:
        logger.warning(f"스키마 조회 실패, 기본 컬럼 사용: {e}")
        return {
            "사업명": "text",
            "사업담당자": "text", 
            "예산": "numeric",
            "기간": "text",
            "상태": "text",
            "등록일자": "date"
        }

def search_invest_excel(search_params: Dict[str, Any]) -> str:
    """
    동적으로 데이터베이스를 검색합니다.
    
    Args:
        search_params: 검색할 컬럼명과 값의 딕셔너리
    
    Returns:
        검색 결과 문자열
    """
    engine = get_db_engine()
    if not engine:
        return "오류: 데이터베이스 연결을 할 수 없습니다."
    
    try:
        schema = get_table_schema()
        available_columns = set(schema.keys())
        
        logger.info(f"검색 파라미터: {search_params}")
        logger.info(f"사용 가능한 컬럼: {available_columns}")
        
        base_query = """
            SELECT * FROM invest_excel
        """
        
        where_clauses = []
        params = {}
        
        param_count = 0
        for column, value in search_params.items():
            if not value or column not in available_columns:
                continue
            
            param_count += 1
            param_key = f"param_{param_count}"
            
            # '단위서비스코드'에 대한 특별 처리
            if column == '단위서비스코드' and isinstance(value, str) and value.startswith('BG0034'):
                where_clauses.append(f'"{column}" LIKE :{param_key}')
                params[param_key] = f"{value}%"
            # '일자'로 끝나는 모든 컬럼에 대해 날짜 형식 처리
            elif column.endswith('일자') and isinstance(value, str):
                year_str = value.split('년')[0]
                if '년 이후' in value:
                    if len(year_str) == 2 and year_str.isdigit():
                        year = int(f"20{year_str}")
                    elif len(year_str) == 4 and year_str.isdigit():
                        year = int(year_str)
                    else:
                        continue
                    
                    corrected_date = f"{year}-01-01"
                    where_clauses.append(f'"{column}" >= :{param_key}')
                    params[param_key] = corrected_date
                else: # '2023년'과 같은 단일 연도 입력 처리
                    if len(year_str) == 2 and year_str.isdigit():
                        year = int(f"20{year_str}")
                    elif len(year_str) == 4 and year_str.isdigit():
                        year = int(year_str)
                    else:
                        continue
                    
                    # 해당 연도의 시작일과 다음 연도의 시작일 미만으로 범위 검색
                    start_date = f"{year}-01-01"
                    end_date = f"{year + 1}-01-01"
                    where_clauses.append(f'"{column}" >= :{param_key}_start AND "{column}" < :{param_key}_end')
                    params[f"{param_key}_start"] = start_date
                    params[f"{param_key}_end"] = end_date
            
            else: # 일반 텍스트나 숫자 컬럼 처리
                column_type = schema.get(column, '').lower()
                if column_type in ['text', 'varchar', 'char', 'character varying']:
                    where_clauses.append(f'"{column}" ILIKE :{param_key}')
                    params[param_key] = f"%{value}%"
                else:
                    where_clauses.append(f'"{column}" = :{param_key}')
                    params[param_key] = value
        
        if where_clauses:
            final_query = text(f"{base_query} WHERE {' AND '.join(where_clauses)}")
            query_params = params
        else:
            final_query = text(f"{base_query} LIMIT 100")
            query_params = {}

        with engine.connect() as conn:
            df = pd.read_sql_query(final_query, conn, params=query_params)
        
        if df.empty:
            return "해당 조건에 맞는 데이터가 없습니다."

        # 변경: 검색 결과 요약 부분을 제거하고, 전체 데이터를 반환하도록 수정
        return df.to_string(index=False)
        
    except Exception as e:
        logger.error(f"데이터베이스 검색 중 오류: {e}")
        return f"데이터베이스 검색 중 오류가 발생했습니다: {str(e)}"

# ----------------- UI 및 기능 추가 -----------------

# Streamlit 페이지 설정
st.set_page_config(
    page_title="IT투자심의 AI Agent",
    page_icon="🏢",
    layout="centered"
)

# 사이드바
with st.sidebar:
    st.header("IT투자심의 AI Agent")
    st.info("이 챗봇은 데이터베이스를 활용하여 답변합니다.")
    
    if st.button("🔄 대화 초기화", use_container_width=True):
        st.session_state.messages = []
        logger.info("대화가 초기화되었습니다.")
        st.rerun()

    # '검색 가능한 키워드' 버튼 추가
    with st.expander("검색 가능한 키워드"):
        schema = get_table_schema()
        if schema:
            st.write("---")
            st.markdown("##### 컬럼명")
            for col in schema.keys():
                st.write(f"- {col}")
        else:
            st.warning("데이터베이스 스키마를 불러올 수 없습니다.")


# 에이전트 프롬프트 설정 (변경: 검색 결과 요약 금지 지시 추가)
agent_prompts = {
    "사업조회": """당신은 IT투자심의 사업 데이터를 전문적으로 조회하는 챗봇입니다.
    사용자의 질문에서 핵심 키워드와 값을 추출하여 데이터베이스를 검색하세요.
    데이터베이스 검색 결과는 요약하지 말고, 전체 내용을 그대로 사용자에게 제공하세요.
    만약 데이터가 없으면 '데이터에 없습니다'라고 답변하세요.""",
}

st.title("IT투자심의 AI Agent")
st.divider()

# 세션 상태 초기화
if 'messages' not in st.session_state:
    st.session_state.messages = []

# 대화 기록 표시
for message in st.session_state.messages:
    if message['role'] != 'system':
        with st.chat_message(message['role']):
            st.write(message['content'])

# 사용자 입력 처리
if user_input := st.chat_input("메시지를 입력하세요"):
    logger.info(f"사용자 입력: {user_input}")
    
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    with st.spinner("응답을 기다리는 중..."):
        try:
            messages_with_context = [
                {"role": "system", "content": agent_prompts["사업조회"]}
            ] + st.session_state.messages

            schema = get_table_schema()
            
            properties = {}
            for col in schema.keys():
                properties[col] = {
                    "type": "string",
                    "description": f"데이터베이스의 '{col}' 컬럼에서 검색할 값"
                }
            
            tools = [{
                "type": "function",
                "function": {
                    "name": "search_invest_excel",
                    "description": """IT투자심의 데이터베이스를 검색합니다.
                    사용자의 질문에서 핵심 키워드와 값을 추출해 검색 파라미터를 생성하세요.
                    이 함수는 검색 결과를 요약하지 않고 원본 데이터를 그대로 반환합니다.
                    예를 들어, 'OOO 사업'이라고 하면 '사업명' 컬럼의 값으로 OOO을 사용합니다.
                    예산 관련 질문은 '예산금액(OPEX)', '예산금액(CAPEX)' 등의 컬럼을 활용하세요.
                    """,
                    "parameters": {
                        "type": "object",
                        "properties": properties,
                        "additionalProperties": False
                    },
                }
            }]

            response = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=messages_with_context,
                tools=tools,
                tool_choice="auto"
            )
            
            message = response.choices[0].message
            
            if message.tool_calls:
                tool_call = message.tool_calls[0]
                function_name = tool_call.function.name
                function_args = json.loads(tool_call.function.arguments)

                # 별칭(alias) 변환 로직
                alias_map = {
                    "사업ID": "투자심의ID",
                    "IT사업ID": "투자심의ID",
                }
                new_function_args = {}
                for key, value in function_args.items():
                    if key in alias_map:
                        new_key = alias_map[key]
                        new_function_args[new_key] = value
                    else:
                        new_function_args[key] = value
                function_args = new_function_args
                
                # 날짜 검색 형식 보정 및 사용자 피드백 로직
                corrected_params = {}
                correction_message = ""
                for key, value in function_args.items():
                    if key.endswith('일자') and isinstance(value, str) and value.endswith('년'):
                        year_str = value.split('년')[0]
                        if year_str.isdigit() and (len(year_str) == 2 or len(year_str) == 4):
                            corrected_value = f"{year_str}년 이후"
                            corrected_params[key] = corrected_value
                            correction_message += f"날짜 검색은 **'{corrected_value}'**와 같은 형식으로 입력해야 합니다. 자동으로 재검색합니다."
                            logger.info(f"날짜 형식 보정: '{value}' -> '{corrected_value}'")
                        else:
                            corrected_params[key] = value
                    else:
                        corrected_params[key] = value
                
                function_args = corrected_params
                
                if correction_message:
                    with st.chat_message("assistant"):
                        st.info(correction_message)
                
                logger.info(f"AI가 추출한 검색 파라미터: {function_args}")
                
                db_data = search_invest_excel(function_args)
                
                tool_messages = [
                    {"role": "assistant", "tool_calls": [{
                        "id": tool_call.id, "type": "function", "function": {
                            "name": tool_call.function.name, "arguments": tool_call.function.arguments,
                        }
                    }]},
                    {"tool_call_id": tool_call.id, "role": "tool", "name": function_name, "content": db_data}
                ]
                
                final_response = client.chat.completions.create(
                    model="gpt-4.1-mini",
                    messages=messages_with_context + tool_messages
                ).choices[0].message.content
                
            else:
                final_response = message.content

            st.session_state.messages.append({"role": "assistant", "content": final_response})
            with st.chat_message("assistant"):
                st.write(final_response)
                
            logger.info(f"AI 응답: {final_response[:100]}...")
                
        except Exception as e:
            logger.error(f"전체 처리 중 오류: {e}")
            error_message = f"처리 중 오류가 발생했습니다: {str(e)}"
            st.session_state.messages.append({"role": "assistant", "content": error_message})
            with st.chat_message("assistant"):
                st.error(error_message)