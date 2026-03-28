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
    score = db.Column(db.Integer, nullable=True)       # normalized 0–100 display score
    risk_score = db.Column(db.Integer, nullable=True)  # raw weighted violation sum
    grade = db.Column(db.String(5), nullable=True)
    result = db.Column(db.String(100))
    inspection_type = db.Column(db.String(100))

    violations = db.relationship(
        'Violation',
        backref='inspection',
        lazy='select',
        cascade='all, delete-orphan'
    )

    @property
    def score_tier(self):
        # Uses raw risk_score to match InspectRI's violationLabel thresholds:
        # <= 2 → low, <= 9 → medium, > 9 → high
        rs = self.risk_score
        if rs is None:
            return None
        if rs <= 2:
            return 'low'
        elif rs <= 9:
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
