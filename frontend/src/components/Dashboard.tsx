import { Shield, Badge, Icon } from "lucide-react";
import { useState } from "react";
import { cn } from '../lib/utils';
import { Link, useLocation } from "wouter";


interface NavItems {
    label: String
    href: String
    icon: String
}
const NavItems: NavItems[] = [
    {label: "Dashboard", href: "/", icon: ""},
    {label: "Funds", href: "/Funds", icon: ""},
    {label: "Holdings", href: "/Holdings", icon: ""},
    {label: "Exceptions", href: "/Exceptions", icon: ""},
    {label: "Upload", href: "/Upload", icon: ""},
    {label: "Audit", href: "/Audit", icon: ""},
]


function Dashboard() {
    const [collapsed, setCollapsed] = useState(false)

    return (
        <>
            <div className="flex h-screen bg-background overflow-hidden">
                {/* Sidebar */}
                <aside
                    className={cn(
                    'flex flex-col h-full border-r border-border transition-all duration-300 ease-in-out shrink-0',
                    'bg-sidebar',
                    collapsed ? 'w-16' : 'w-60'
                    )}
                >

                    {/* Logo */}
                    <div className={cn('flex items-center h-14 px-4 border-b border-border shrink-0', collapsed && 'justify-center px-0')}>
                    {collapsed ? (
                        <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
                        <Shield className="w-4 h-4 text-primary" />
                        </div>
                    ) : (
                        <div className="flex items-center gap-2.5">
                        <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center shrink-0">
                            <Shield className="w-4 h-4 text-primary" />
                        </div>
                        <div className="min-w-0">
                            <p className="text-sm font-semibold text-foreground leading-tight truncate" style={{ fontFamily: 'var(--font-display)' }}>ETF Compliance</p>
                            <p className="text-[10px] text-muted-foreground leading-tight">Filing System</p>
                        </div>
                        </div>
                    )}
                    </div>
                
                    {/* NAvigation */}
                    <nav className="flex-1 overflow-y-auto py-3 px-2 space-y-0.5">
                        { 
                            NavItems.map( (item) => {
                                const icon = item
                                const isActive = true;
                                
                                return (
                                    <Link key={item.label} href={item.href}>
                                        <div
                                            className={cn(
                                                'relative flex items-center gap-2.5 px-3 py-2 rounded-lg transition-all duration-150 group',
                                                isActive
                                                ? 'bg-primary/15 text-primary'
                                                : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                                            )}
                                            >
                                            {isActive && (
                                                <span className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 bg-primary rounded-r-full" />
                                            )}
                                            <span className="text-sm font-medium flex-1 truncate" style={{ fontFamily: 'var(--font-display)' }}>
                                                {item.label}
                                            </span>
                                        </div>


                                    </Link>
                                )



                            })
                        }
                    </nav>

                </aside>
            </div>
        
        </>
    )
}

export default Dashboard;