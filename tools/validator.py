import os
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator, HttpUrl


# ===== Входные данные =====

class GigaChatConfig(BaseModel):
    """Валидация конфигурации GigaChat"""
    base_url: HttpUrl = Field(..., description="Base URL for API")
    cert_file: str = Field(..., description="Certificate file path")
    key_file: str = Field(..., description="Key file path")
    verify_ssl_certs: bool = Field(default=False)
    model: str = Field(..., description="Model name")
    verbose: bool = Field(default=False)
    temperature: float = Field(ge=0.0, le=2.0, default=0.1)
    timeout: int = Field(gt=0, default=1000)

    @validator('cert_file', 'key_file')
    def validate_file_paths(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"File {v} does not exist")
        return v


class TopicGenerationRequest(BaseModel):
    """Валидация запроса для генерации топиков"""
    message: str = Field(...)
    user_message: str = Field(...)
    system_message: str = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    count_blacklist_attemps: int = Field(ge=1, le=10, default=1)


class ThemeGenerationRequest(BaseModel):
    """Валидация запроса для генерации тем"""
    message: List[str] = Field(default_factory=list)
    user_message: str = Field(...)
    system_message: str = Field(...)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    count_blacklist_attemps: int = Field(ge=1, le=10, default=1)
    recursion_depth: int = Field(ge=1, le=10, default=3)
    cluster_topics: List[str] = Field(default_factory=list)
    cluster_name: str = Field(...)
    void_return_check: bool = Field(default=True)


class AnalyticsRequest(BaseModel):
    """Валидация запроса для аналитики"""
    user_message: str = Field(...)
    system_message: str = Field(...)
    region_name: str = Field(...)
    region_themes: List[str] = Field(...)
    russia_themes: Optional[List[str]] = Field(default_factory=list)
    cluster_name: str = Field(...)
    count_blacklist_attemps: int = Field(ge=1, le=10, default=1)


class SummerizeRequest(BaseModel):
    """Валидация запроса для суммаризации"""
    user_message: str = Field(...)
    system_message: str = Field(...)
    region_name: str = Field(...)
    category_conclusions: List[Dict[str, Any]] = Field(...)
    count_blacklist_attemps: int = Field(ge=1, le=10, default=1)


class ClusterizationRequest(BaseModel):
    """Валидация запроса для кластеризации"""
    message: str = Field(...)
    user_message: str = Field(...)
    system_message: str = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    count_blacklist_attemps: int = Field(ge=1, le=10, default=1)
    categories_cluster: Optional[List[str]] = Field(default_factory=list)


class MergeThemesRequest(BaseModel):
    """Валидация запроса для объединения тем"""
    themes: List[str] = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ===== Выходные данные =====

class TopicResponse(BaseModel):
    """Валидация ответа с топиками"""
    topics: List[str] = Field(...)


class ThemeResponse(BaseModel):
    """Валидация ответа с темами"""
    themes: List[str] = Field(...)


class AnalyticsResponse(BaseModel):
    """Валидация аналитического ответа для сравнения региона с РФ"""
    conclusion_region: str = Field(...)
    sentiment_region: str = Field(...)
    conclusion_russia: str = Field(...)
    sentiment_russia: str = Field(...)

    @validator('sentiment_region')
    def validate_sentiment_region(cls, v):
        valid_sentiments = {'positive', 'negative', 'neutral', 'mixed'}
        if v.lower() in valid_sentiments:
            return v.lower()
        print(f"⚠️  Некорректный sentiment_region: '{v}'. Установлено значение 'neutral'")
        return 'neutral'

    @validator('sentiment_russia')
    def validate_sentiment_russia(cls, v, values):
        valid_sentiments = {'positive', 'negative', 'neutral', 'mixed'}
        conclusion_russia = values.get('conclusion_russia', '')

        if not conclusion_russia:
            return ''  # Пустая строка, если нет вывода по России

        if v.lower() in valid_sentiments:
            return v.lower()

        print(f"⚠️  Некорректный sentiment_russia: '{v}'. Установлено значение 'neutral'")
        return 'neutral'


class ClusterResponseItem(BaseModel):
    """Валидация элемента кластеризации"""
    cluster_name: str = Field(...)
    topics: List[Dict[str, Any]] = Field(...)


class SummerizeResponse(BaseModel):
    """Валидация ответа суммаризации"""
    conclusion_region: str = Field(...)
    sentiment_region: str = Field(...)
    conclusion_russia: str = Field(...)
    sentiment_russia: str = Field(...)

    @validator('sentiment_region')
    def validate_sentiment_region(cls, v):
        valid_sentiments = {'positive', 'negative', 'neutral', 'mixed'}
        if v.lower() in valid_sentiments:
            return v.lower()
        print(f"⚠️  Некорректный sentiment_region: '{v}'. Установлено значение 'neutral'")
        return 'neutral'

    @validator('sentiment_russia')
    def validate_sentiment_russia(cls, v, values):
        valid_sentiments = {'positive', 'negative', 'neutral', 'mixed'}
        conclusion_russia = values.get('conclusion_russia', '')

        if not conclusion_russia:
            return ''

        if v.lower() in valid_sentiments:
            return v.lower()

        print(f"⚠️  Некорректный sentiment_russia: '{v}'. Установлено значение 'neutral'")
        return 'neutral'


# ===== Обработчики входных данных =====

class TopicGenerationInput:
    """Обработчик входных данных для генерации топиков"""

    def __init__(self, message: str, user_message: str,
                 system_message: str, metadata: dict = None,
                 count_blacklist_attemps: int = 1):
        self.raw_data = {
            'message': message,
            'user_message': user_message,
            'system_message': system_message,
            'metadata': metadata or {},
            'count_blacklist_attemps': count_blacklist_attemps
        }
        self.validated_data = Validator.validate_topic_generation(**self.raw_data)

    def get_validated(self) -> TopicGenerationRequest:
        return self.validated_data


class ThemeGenerationInput:
    """Обработчик входных данных для генерации тем"""

    def __init__(self, message: list['str'], user_message: str,
                 system_message: str, metadata: dict = None,
                 count_blacklist_attemps: int = 1, recursion_depth: int = 3, **kwargs):
        self.raw_data = {
            'message': message,
            'user_message': user_message,
            'system_message': system_message,
            'metadata': metadata or {},
            'count_blacklist_attemps': count_blacklist_attemps,
            'recursion_depth': recursion_depth,
            **kwargs
        }
        self.validated_data = Validator.validate_theme_generation(**self.raw_data)

    def get_validated(self) -> ThemeGenerationRequest:
        return self.validated_data


class AnalyticsInput:
    """Обработчик входных данных для аналитики"""

    def __init__(self, user_message: str, system_message: str, region_name: str,
                 region_themes: List[str], russia_themes: List[str], cluster_name: str,
                 count_blacklist_attemps: int = 1):
        self.raw_data = {
            'user_message': user_message,
            'system_message': system_message,
            'region_name': region_name,
            'region_themes': region_themes,
            'russia_themes': russia_themes,
            'cluster_name': cluster_name,
            'count_blacklist_attemps': count_blacklist_attemps
        }
        self.validated_data = Validator.validate_analytics(**self.raw_data)

    def get_validated(self) -> AnalyticsRequest:
        return self.validated_data


class SummerizeInput:
    """Обработчик входных данных для суммаризации"""

    def __init__(self, user_message: str, system_message: str, region_name: str,
                 category_conclusions: List[Dict], count_blacklist_attemps: int = 1):
        self.raw_data = {
            'user_message': user_message,
            'system_message': system_message,
            'region_name': region_name,
            'category_conclusions': category_conclusions,
            'count_blacklist_attemps': count_blacklist_attemps
        }
        self.validated_data = Validator.validate_summerize(**self.raw_data)

    def get_validated(self) -> SummerizeRequest:
        return self.validated_data


class ClusterizationInput:
    """Обработчик входных данных для кластеризации"""

    def __init__(self, message: str, user_message: Optional[str] = None,
                 system_message: Optional[str] = None, metadata: dict = None,
                 count_blacklist_attemps: int = 1, **kwargs):
        self.raw_data = {
            'message': message,
            'user_message': user_message,
            'system_message': system_message,
            'metadata': metadata or {},
            'count_blacklist_attemps': count_blacklist_attemps,
            'categories_cluster': kwargs.get('CATEGORIES_CLUSTER', [])
        }
        self.validated_data = Validator.validate_clusterization(**self.raw_data)

    def get_validated(self) -> ClusterizationRequest:
        return self.validated_data


class MergeThemesInput:
    """Обработчик входных данных для объединения тем"""

    def __init__(self, themes: List[str], metadata: dict = None):
        self.raw_data = {
            'themes': themes,
            'metadata': metadata or {}
        }
        self.validated_data = Validator.validate_merge_themes(**self.raw_data)

    def get_validated(self) -> MergeThemesRequest:
        return self.validated_data


# ===== Обработчики выходных данных =====

class TopicOutput:
    """Обработчик выходных данных для топиков"""

    def __init__(self, response_data: dict):
        self.raw_data = response_data
        try:
            self.validated_data = Validator.validate_topic_response(response_data)
        except Exception as e:
            self.validated_data = None
            self.error = str(e)
        else:
            self.error = None

    def get_validated(self) -> Optional[TopicResponse]:
        return self.validated_data

    def is_valid(self) -> bool:
        return self.validated_data is not None


class ThemeOutput:
    """Обработчик выходных данных для тем"""

    def __init__(self, response_data: dict):
        self.raw_data = response_data
        try:
            self.validated_data = Validator.validate_theme_response(response_data)
        except Exception as e:
            self.validated_data = None
            self.error = str(e)
        else:
            self.error = None

    def get_validated(self) -> Optional[ThemeResponse]:
        return self.validated_data

    def is_valid(self) -> bool:
        return self.validated_data is not None


class AnalyticsOutput:
    """Обработчик выходных данных для аналитики"""

    def __init__(self, response_data: dict):
        self.raw_data = response_data
        try:
            self.validated_data = Validator.validate_analytics_response(response_data)
        except Exception as e:
            self.validated_data = None
            self.error = str(e)
        else:
            self.error = None

    def get_validated(self) -> Optional[AnalyticsResponse]:
        return self.validated_data

    def is_valid(self) -> bool:
        return self.validated_data is not None


class SummerizeOutput:
    """Обработчик выходных данных для суммаризации"""

    def __init__(self, response_data: dict):
        self.raw_data = response_data
        try:
            self.validated_data = Validator.validate_summerize_response(response_data)
        except Exception as e:
            self.validated_data = None
            self.error = str(e)
        else:
            self.error = None

    def get_validated(self) -> Optional[SummerizeResponse]:
        return self.validated_data

    def is_valid(self) -> bool:
        return self.validated_data is not None


# ===== Основной валидатор =====

class Validator:
    """Основной класс валидатора"""

    @staticmethod
    def validate_config(**kwargs) -> GigaChatConfig:
        return GigaChatConfig(**kwargs)

    @staticmethod
    def validate_topic_generation(**kwargs) -> TopicGenerationRequest:
        return TopicGenerationRequest(**kwargs)

    @staticmethod
    def validate_theme_generation(**kwargs) -> ThemeGenerationRequest:
        return ThemeGenerationRequest(**kwargs)

    @staticmethod
    def validate_analytics(**kwargs) -> AnalyticsRequest:
        return AnalyticsRequest(**kwargs)

    @staticmethod
    def validate_summerize(**kwargs) -> SummerizeRequest:
        return SummerizeRequest(**kwargs)

    @staticmethod
    def validate_clusterization(**kwargs) -> ClusterizationRequest:
        return ClusterizationRequest(**kwargs)

    @staticmethod
    def validate_merge_themes(**kwargs) -> MergeThemesRequest:
        return MergeThemesRequest(**kwargs)

    @staticmethod
    def validate_topic_response(response: dict) -> TopicResponse:
        return TopicResponse(**response)

    @staticmethod
    def validate_theme_response(response: dict) -> ThemeResponse:
        return ThemeResponse(**response)

    @staticmethod
    def validate_analytics_response(response: dict) -> AnalyticsResponse:
        return AnalyticsResponse(**response)

    @staticmethod
    def validate_summerize_response(response: dict) -> SummerizeResponse:
        return SummerizeResponse(**response)

    @staticmethod
    def validate_system_message(system_message: str) -> bool:
        if not system_message or not system_message.strip():
            return False
        return True