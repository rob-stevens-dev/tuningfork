"""
Recommendation module for TuningFork database performance optimization tool.

This module provides the data model for database optimization recommendations.
"""

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional


class RecommendationPriority(enum.Enum):
    """Priority levels for recommendations."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class RecommendationType(enum.Enum):
    """Types of recommendations."""
    CONFIGURATION = "configuration"
    SCHEMA = "schema"
    QUERY = "query"
    RESOURCE = "resource"
    INDEX = "index"
    MAINTENANCE = "maintenance"
    SECURITY = "security"
    OTHER = "other"


@dataclass
class Recommendation:
    """
    Data model for database optimization recommendations.
    
    Attributes:
        id: Unique identifier for the recommendation
        title: Short title describing the recommendation
        description: Detailed description of the recommendation
        priority: Priority level of the recommendation
        type: Type of recommendation
        implementation_script: SQL or command to implement the recommendation
        expected_benefit: Description of the expected benefits
        risk_level: Level of risk associated with implementing the recommendation
        risk_details: Detailed description of the risks
        estimated_time: Estimated time to implement the recommendation
        category: Category for grouping similar recommendations
        timestamp: When the recommendation was generated
        implemented: Whether the recommendation has been implemented
        implementation_time: When the recommendation was implemented
        implementation_notes: Notes about the implementation
        verification_status: Status of the verification process
        verification_notes: Notes about the verification process
        before_metrics: Metrics before implementation
        after_metrics: Metrics after implementation
    """
    
    title: str
    description: str
    priority: RecommendationPriority
    type: RecommendationType
    implementation_script: str
    expected_benefit: str
    risk_level: str
    risk_details: str
    estimated_time: str
    category: str
    
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    implemented: bool = False
    implementation_time: Optional[datetime] = None
    implementation_notes: Optional[str] = None
    verification_status: Optional[str] = None
    verification_notes: Optional[str] = None
    before_metrics: Dict[str, Any] = field(default_factory=dict)
    after_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the recommendation to a dictionary.
        
        Returns:
            Dictionary representation of the recommendation
        """
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "priority": self.priority.value,
            "type": self.type.value,
            "implementation_script": self.implementation_script,
            "expected_benefit": self.expected_benefit,
            "risk_level": self.risk_level,
            "risk_details": self.risk_details,
            "estimated_time": self.estimated_time,
            "category": self.category,
            "timestamp": self.timestamp.isoformat(),
            "implemented": self.implemented,
            "implementation_time": self.implementation_time.isoformat() if self.implementation_time else None,
            "implementation_notes": self.implementation_notes,
            "verification_status": self.verification_status,
            "verification_notes": self.verification_notes,
            "before_metrics": self.before_metrics,
            "after_metrics": self.after_metrics
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Recommendation':
        """
        Create a recommendation from a dictionary.
        
        Args:
            data: Dictionary representation of the recommendation
            
        Returns:
            Recommendation instance
        """
        # Convert string representations back to enums
        priority = RecommendationPriority(data.get("priority", "medium"))
        rec_type = RecommendationType(data.get("type", "other"))
        
        # Convert string timestamps back to datetime
        timestamp = datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat()))
        implementation_time = None
        if data.get("implementation_time"):
            implementation_time = datetime.fromisoformat(data["implementation_time"])
        
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            title=data.get("title", ""),
            description=data.get("description", ""),
            priority=priority,
            type=rec_type,
            implementation_script=data.get("implementation_script", ""),
            expected_benefit=data.get("expected_benefit", ""),
            risk_level=data.get("risk_level", ""),
            risk_details=data.get("risk_details", ""),
            estimated_time=data.get("estimated_time", ""),
            category=data.get("category", ""),
            timestamp=timestamp,
            implemented=data.get("implemented", False),
            implementation_time=implementation_time,
            implementation_notes=data.get("implementation_notes"),
            verification_status=data.get("verification_status"),
            verification_notes=data.get("verification_notes"),
            before_metrics=data.get("before_metrics", {}),
            after_metrics=data.get("after_metrics", {})
        )