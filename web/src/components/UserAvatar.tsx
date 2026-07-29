import { Link } from "react-router-dom";
import { User as UserIcon } from "lucide-react";
import clsx from "clsx";

interface Props {
  userID?: number;
  username?: string;
  avatarUrl?: string;
  size?: number; // px
  clickable?: boolean;
  className?: string;
}

export default function UserAvatar({
  userID,
  username,
  avatarUrl,
  size = 40,
  clickable = true,
  className,
}: Props) {
  const inner = avatarUrl ? (
    <img
      src={avatarUrl}
      alt={username || "avatar"}
      style={{ width: size, height: size }}
      className={clsx("rounded-full object-cover bg-gray-100", className)}
    />
  ) : (
    <div
      style={{ width: size, height: size }}
      className={clsx(
        "rounded-full bg-brand-100 text-brand-600 flex items-center justify-center",
        className
      )}
    >
      <UserIcon size={Math.max(12, size / 2)} />
    </div>
  );

  if (clickable && userID) {
    return (
      <Link to={`/users/${userID}`} className="inline-block shrink-0">
        {inner}
      </Link>
    );
  }
  return <div className="inline-block shrink-0">{inner}</div>;
}
