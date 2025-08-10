import json
import os
import datetime
from typing import Dict, Any, Optional, List
import pandas as pd
from .Notifier import Notifier


class ReportManager:
    """데이터 업데이트 작업의 결과를 수집하고 통합 리포트를 생성하는 클래스"""
    
    def __init__(self, report_dir: str = "reports"):
        """
        ReportManager 초기화
        
        Args:
            report_dir (str): 리포트 파일들을 저장할 디렉토리
        """
        self.report_dir = report_dir
        self.ensure_report_dir()
        self.notifier = Notifier()
        
        self.job_names = [
            "bills",
            "lawmakers",
            "timeline",
            "votes",
            "results"
        ]
        
        # 상태 이모지 매핑
        self.status_emojis = {
            "success": "✅",
            "no_change": "⚪",
            "no_data": "➖",
            "error": "🚨",
            "failure": "❌",
        }
    
    def ensure_report_dir(self):
        """리포트 디렉토리가 존재하는지 확인하고 없으면 생성"""
        if not os.path.exists(self.report_dir):
            os.makedirs(self.report_dir)
    
    def save_job_result(self, job_name: str, status: str, data_count: int = 0, 
                       error_message: str = None, execution_time: float = 0,
                       data_distribution: Dict[str, Any] = None):
        """
        개별 작업의 결과를 저장
        
        Args:
            job_name (str): 작업 이름 (lawmakers, bills, timeline, votes, results)
            status (str): 작업 상태 (success, failure, error, no_data)
            data_count (int): 처리된 데이터 개수
            error_message (str): 에러 메시지 (에러 발생시)
            execution_time (float): 실행 시간 (초)
            data_distribution (Dict): 데이터 분포 정보
        """
        result = {
            "job_name": job_name,
            "status": status,
            "data_count": data_count,
            "error_message": error_message,
            "execution_time": execution_time,
            "data_distribution": data_distribution or {},
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        result_file = os.path.join(self.report_dir, f"{job_name}_result.json")
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    
    def get_job_result(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        개별 작업의 결과를 조회
        
        Args:
            job_name (str): 작업 이름
            
        Returns:
            Dict: 작업 결과 딕셔너리 또는 None
        """
        result_file = os.path.join(self.report_dir, f"{job_name}_result.json")
        if os.path.exists(result_file):
            with open(result_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def collect_all_results(self) -> Dict[str, Dict[str, Any]]:
        """
        모든 작업의 결과를 수집
        
        Returns:
            Dict: 작업명을 키로 하는 결과 딕셔너리
        """
        results = {}
        for job_name in self.job_names:
            result = self.get_job_result(job_name)
            if result:
                results[job_name] = result
        return results
    
    def generate_status_report(self) -> str:
        """
        실행 순서에 따른 상태 리포트 메시지를 생성합니다.
        
        Returns:
            str: 디스코드로 전송할 상태 리포트 메시지.
        """
        results = self.collect_all_results()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        
        report_lines = [f"📊 **데이터 업데이트 요약 리포트** ({current_time})"]
        
        if not results:
            return ""

        for job_key in self.job_names:
            if job_key in results:
                result = results[job_key]
                status = result['status']
                data_count = result.get('data_count', 0)
                emoji = self.status_emojis.get(status, "❓")
                
                if status == "success":
                    line = f"{emoji} **{job_key}**: 전송 성공 ({data_count}건)"
                elif status == "no_change":
                    line = f"{emoji} **{job_key}**: 변경사항 없음(전송 생략)"
                elif status == "no_data":
                    line = f"{emoji} **{job_key}**: 수집 데이터 없음"
                elif status == "error":
                    line = f"🚨 **{job_key}**: 실행 오류"
                else:
                    line = f"❓ **{job_key}**: 알 수 없는 상태"
                
                report_lines.append(line)
        
        return "\n".join(report_lines)

    def send_status_report(self):
        """상태 리포트를 생성하고 디스코드로 전송합니다."""
        report_message = self.generate_status_report()
        if report_message:
            self.notifier.send_discord_message(report_message)
    
    def generate_distribution_report(self) -> List[str]:
        """
        데이터 분포 리포트 메시지들 생성 (처리된 데이터가 있는 경우만)
        
        Returns:
            List[str]: 디스코드로 전송할 분포 리포트 메시지들
        """
        results = self.collect_all_results()
        distribution_messages = []
        
        for job_name, result in results.items():
            if (result.get('status') == 'success' and 
                result.get('data_count', 0) > 0 and 
                result.get('data_distribution')):
                
                message_lines = [f"📈 **{job_name} 분포 상세** ({result['data_count']}건)"]
                
                for dist_name, dist_data in result['data_distribution'].items():
                    message_lines.append(f"\n[{dist_name}]")
                    if isinstance(dist_data, dict):
                        for key, value in dist_data.items():
                            message_lines.append(f"{key}    {value}")
                    else:
                        message_lines.append(str(dist_data))
                
                distribution_messages.append("\n".join(message_lines))
        
        return distribution_messages
    
    def send_integrated_report(self):
        """통합 리포트를 디스코드로 전송"""
        # 1. 상태 리포트 전송
        self.send_status_report()
        
        # 2. 분포 리포트들 전송 (데이터가 있는 경우만)
        distribution_reports = self.generate_distribution_report()
        for report in distribution_reports:
            self.notifier.send_discord_message(report)
    
    def clear_results(self):
        """모든 결과 파일들을 삭제"""
        for job_name in self.job_names:
            result_file = os.path.join(self.report_dir, f"{job_name}_result.json")
            if os.path.exists(result_file):
                os.remove(result_file)
    
    def calculate_data_distribution(self, df: pd.DataFrame, job_name: str) -> Dict[str, Any]:
        """
        데이터프레임의 분포 정보를 계산
        
        Args:
            df (pd.DataFrame): 분석할 데이터프레임
            job_name (str): 작업 이름
            
        Returns:
            Dict: 분포 정보 딕셔너리
        """
        if df is None or len(df) == 0:
            return {}
        
        distribution = {}
        
        if job_name == "bills":
            # 법안 데이터의 경우 제안일자별, 발의주체별 분포
            if 'proposeDate' in df.columns:
                propose_dist = df['proposeDate'].value_counts().head(10).to_dict()
                distribution['법안 제안일자별 분포'] = propose_dist
            
            if 'proposerKind' in df.columns:
                proposer_dist = df['proposerKind'].value_counts().to_dict()
                distribution['법안 발의주체별 분포'] = proposer_dist
                
        elif job_name == "lawmakers":
            # 의원 데이터의 경우 정당별, 선거구별 분포
            if 'partyName' in df.columns:
                party_dist = df['partyName'].value_counts().head(10).to_dict()
                distribution['정당별 분포'] = party_dist
                
        elif job_name == "votes":
            # 표결 데이터의 경우 날짜별, 결과별 분포
            if 'voteDate' in df.columns:
                vote_date_dist = df['voteDate'].value_counts().head(10).to_dict()
                distribution['표결 날짜별 분포'] = vote_date_dist
                
        elif job_name == "timeline":
            # 타임라인 데이터의 경우 단계별 분포
            if 'procStage' in df.columns:
                stage_dist = df['procStage'].value_counts().to_dict()
                distribution['처리 단계별 분포'] = stage_dist
                
        elif job_name == "results":
            # 처리결과 데이터의 경우 결과별 분포
            if 'procResult' in df.columns:
                result_dist = df['procResult'].value_counts().to_dict()
                distribution['처리 결과별 분포'] = result_dist
        
        return distribution
