from datetime import date, timedelta

from app.db import db


class Inspection(db.Model):
    __tablename__ = 'inspections'

    id = db.Column(db.Integer, primary_key=True)
    restaurant_id = db.Column(
        db.Integer,
        db.ForeignKey('restaurants.id', ondelete='CASCADE'),
        nullable=False
    )
    inspection_date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.String(50))               # portal inspection ID (e.g. INSP-12345)
    score = db.Column(db.Integer, nullable=True)       # normalized 0–100 display score
    risk_score = db.Column(db.Numeric(6, 2), nullable=True)  # raw weighted violation sum
    grade = db.Column(db.String(5), nullable=True)
    result = db.Column(db.String(100))
    inspection_type = db.Column(db.String(100))
    region = db.Column(db.String(100), nullable=True)

    __table_args__ = (
        db.Index('ix_inspections_restaurant_id', 'restaurant_id'),
        db.Index('ix_inspections_restaurant_date', 'restaurant_id', 'inspection_date'),
        db.Index('ix_inspections_restaurant_date_score', 'restaurant_id', 'inspection_date', 'risk_score'),
        db.Index('ix_inspections_date', 'inspection_date'),
        db.Index('ix_inspections_score', 'score'),
        db.Index('ix_inspections_date_score', 'inspection_date', 'score'),
        db.Index('ix_inspections_region', 'region'),
        db.Index('ix_inspections_region_date', 'region', 'inspection_date'),
    )

    @classmethod
    def not_future(cls):
        """Filter clause that excludes inspections dated more than 1 day in the future."""
        return cls.inspection_date <= date.today() + timedelta(days=1)

    violations = db.relationship(
        'Violation',
        backref='inspection',
        lazy='select',
        cascade='all, delete-orphan'
    )

    @property
    def score_tier(self):
        # Score-based thresholds: >= 75 = low, 55–74 = medium, < 55 = high
        s = self.score
        if s is None:
            return None
        if s >= 75:
            return 'low'
        elif s >= 55:
            return 'medium'
        else:
            return 'high'

    @property
    def score_css_class(self):
        return self.score_tier or ''

    @property
    def violation_summary(self):
        if not self.violations:
            return 'No violations found.'
        counts = {'critical': 0, 'major': 0, 'minor': 0}
        corrected = 0
        for v in self.violations:
            severity = v.severity if v.severity in counts else 'minor'
            counts[severity] += 1
            if v.corrected_on_site:
                corrected += 1
        parts = []
        if counts['critical']:
            parts.append(f"{counts['critical']} critical violation{'s' if counts['critical'] != 1 else ''}")
        if counts['major']:
            parts.append(f"{counts['major']} major violation{'s' if counts['major'] != 1 else ''}")
        if counts['minor']:
            parts.append(f"{counts['minor']} minor violation{'s' if counts['minor'] != 1 else ''}")
        summary = '. '.join(parts) + '.'
        if corrected:
            summary += f' {corrected} corrected on site.'
        return summary

    def __repr__(self):
        return f'<Inspection {self.inspection_date} score={self.score}>'
